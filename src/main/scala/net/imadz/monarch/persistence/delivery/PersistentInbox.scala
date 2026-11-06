package net.imadz.monarch.persistence.delivery

import akka.actor.Cancellable
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, BehaviorInterceptor, BehaviorSignalInterceptor, PostStop, PreRestart, Signal, TypedActorContext}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted}
import net.imadz.monarch.{RunLease, StaleRun}
import net.imadz.monarch.persistence.{RunProtocol, RunSubmissionPort}
import net.imadz.monarch.persistence.delivery.DeliveryProtocol._

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** Durable, single-child inbox. Accepted is responsibility, not completion.
  *
  * A port timeout persists Blocked but cannot cancel its Future: the actual call
  * slot stays occupied until completion. Reconcile cannot bypass that hold.
  * Recreation clears only local ownership, never an external call. Hosts must
  * bound entity cardinality and must not recreate owners to accumulate hung I/O.
  * RunLease fencing is JVM-local; routing and single-writer journals remain host
  * obligations. Child completion is read from RunProtocol, never an observer.
  */
object PersistentInbox {
  private final case class LiveCall(id: Long, kind: String, future: Future[RunProtocol.Reply])
  private[persistence] def leaseKey(id: PersistenceId): String = s"monarch:delivery:${id.id}"

  def behavior(persistenceId: PersistenceId, deliveryId: String, runs: RunSubmissionPort,
      settings: DeliverySettings = DeliverySettings())(implicit ec: ExecutionContext): Behavior[Command] =
    behaviorWithClock(persistenceId, deliveryId, runs, settings, () => System.currentTimeMillis())

  private[persistence] def behaviorWithClock(persistenceId: PersistenceId, deliveryId: String,
      runs: RunSubmissionPort, settings: DeliverySettings, clock: () => Long)
      (implicit ec: ExecutionContext): Behavior[Command] = Behaviors.setup { ctx =>
    require(runs != null && settings != null, "delivery run port and settings are required")
    val limits = settings.limits
    DeliveryValidation.requireValid(limits.text("delivery id", deliveryId))
    val self = ctx.self
    val scheduler = ctx.system.scheduler

    final class Owner {
      private val ioTimer = new AtomicReference[Cancellable]()
      private val pollTimer = new AtomicReference[Cancellable]()
      val lease: RunLease = RunLease.acquire(leaseKey(persistenceId), () => cancelTimers())
      private def cancel(slot: AtomicReference[Cancellable]): Unit = Option(slot.getAndSet(null)).foreach(_.cancel())
      private def install(slot: AtomicReference[Cancellable], task: Cancellable): Unit = {
        Option(slot.getAndSet(task)).foreach(_.cancel())
        if (!lease.isFresh && slot.compareAndSet(task, null)) task.cancel()
      }
      def setIO(task: Cancellable): Unit = install(ioTimer, task)
      def setPoll(task: Cancellable): Unit = install(pollTimer, task)
      def cancelIO(): Unit = cancel(ioTimer)
      def cancelPoll(): Unit = cancel(pollTimer)
      def cancelTimers(): Unit = { cancelIO(); cancelPoll() }
      def close(): Unit = { lease.revoke(); cancelTimers() }
    }

    val owner = new Owner
    val generation = owner.lease.generation
    var callSequence = 0L
    var timerToken = 0L
    var inFlight: Option[LiveCall] = None
    var localProblem: Option[String] = None

    def fresh(g: Long): Boolean = g == generation && owner.lease.isFresh
    def close(): Unit = { owner.close(); inFlight = None }
    def problem(state: DeliveryState): Option[String] = localProblem
      .orElse(if (owner.lease.isFresh) None else Some("delivery owner has been revoked"))
      .orElse(DeliveryState.recoveryProblem(state, deliveryId, limits))

    def diagnostic(state: DeliveryState, message: String): String = {
      val nonempty = Option(message).filter(_.nonEmpty).getOrElse("child outcome remains unavailable")
      limits.diagnostic(state.policy.fold(nonempty)(_.limits.diagnostic(nonempty)))
    }

    def view(state: DeliveryState): View = {
      val blocked = problem(state)
      View(deliveryId, state.requestDigest, state.request.map(_.child.runId),
        if (blocked.nonEmpty) Phase.RecoveryBlocked else state.phase,
        if (blocked.nonEmpty) None else state.result, blocked.orElse(state.reason).map(diagnostic(state, _)),
        reconciling = blocked.isEmpty && state.phase == Phase.Blocked && state.nextAction.contains(DeliveryState.Query))
    }

    def reject(replyTo: ActorRef[Reply], key: String, reason: String): Unit =
      if (replyTo != null) replyTo ! Rejected(
        if (limits.text("delivery id", key).isEmpty) key else deliveryId, limits.diagnostic(reason))

    def commit(state: DeliveryState, event: DeliveryEvent)(after: DeliveryState => Unit)
        : Effect[DeliveryEvent, DeliveryState] = {
      if (!owner.lease.isFresh) Effect.none
      else {
        val candidate = DeliveryState.evolve(state, event)
        val invalid = DeliveryValidation.event(event, limits)
          .orElse(candidate.invalid).orElse(DeliveryValidation.state(candidate, limits))
        invalid match {
          case Some(reason) =>
            localProblem = Some(diagnostic(state, reason))
            owner.cancelTimers()
            ctx.log.error("Delivery {} blocked before an invalid journal transition: {}", deliveryId, reason)
            Effect.none
          case None => Effect.persist(event).thenRun(after)
        }
      }
    }

    def startCall(state: DeliveryState, kind: String): Unit = {
      if (problem(state).isEmpty && !state.terminal && inFlight.isEmpty) {
        owner.cancelPoll()
        val child = state.request.get.child
        val lease = owner.lease
        callSequence += 1L
        val id = callSequence
        // The invocation itself may block before returning a Future. Flatten it
        // on the actor dispatcher; the watchdog never uses the supplied I/O EC.
        val future: Future[RunProtocol.Reply] = try {
          Future {
            if (!lease.isFresh) Future.failed[RunProtocol.Reply](StaleRun)
            else if (kind == DeliveryState.Ensure) runs.ensure(child)
            else runs.query(child)
          }(ec).flatMap { result =>
            if (result == null) Future.failed(new IllegalStateException("run port returned a null Future")) else result
          }(ctx.executionContext)
        } catch { case NonFatal(ex) => Future.failed(ex) }
        inFlight = Some(LiveCall(id, kind, future))
        owner.setIO(scheduler.scheduleOnce(state.policy.get.queryTimeoutMillis.millis, new Runnable {
          override def run(): Unit = self ! CallTimedOut(generation, id)
        })(ctx.executionContext))
        future.onComplete(outcome => self ! CallFinished(generation, id, outcome))(ctx.executionContext)
      }
    }

    def schedule(state: DeliveryState): Unit = {
      if (problem(state).isEmpty && !state.terminal && inFlight.isEmpty) state.nextPollAt.foreach { due =>
        timerToken += 1L
        val token = timerToken
        val delay = math.max(0L, math.min(DeliverySettings.MaxDelay.toMillis, due - clock())).millis
        owner.setPoll(scheduler.scheduleOnce(delay, new Runnable {
          override def run(): Unit = self ! PollDue(generation, token, due)
        })(ctx.executionContext))
      }
    }

    def block(state: DeliveryState, reason: String): Effect[DeliveryEvent, DeliveryState] = {
      owner.cancelPoll()
      commit(state, DeliveryEvent.Blocked(diagnostic(state, reason)))(_ => ())
    }

    def ensureChild(state: DeliveryState): Effect[DeliveryEvent, DeliveryState] =
      if (state.starts >= state.policy.get.maxStarts) block(state, DeliveryState.StartsExhausted)
      else commit(state, DeliveryEvent.ChildStartIntent(state.starts + 1))(next => startCall(next, DeliveryState.Ensure))

    def drive(state: DeliveryState): Effect[DeliveryEvent, DeliveryState] = {
      if (problem(state).nonEmpty || state.terminal || inFlight.nonEmpty) Effect.none
      else state.nextAction match {
        case Some(DeliveryState.Ensure) => ensureChild(state)
        case Some(DeliveryState.Query) if state.nextPollAt.exists(_ > clock()) => schedule(state); Effect.none
        case Some(DeliveryState.Query) if state.remainingPolls <= 0 => block(state, DeliveryState.Exhausted)
        case Some(DeliveryState.Query) =>
          commit(state, DeliveryEvent.ChildQueryIntent(state.polls + 1))(next => startCall(next, DeliveryState.Query))
        case _ => Effect.none
      }
    }

    def observed(state: DeliveryState, call: LiveCall, reply: RunProtocol.Reply): Effect[DeliveryEvent, DeliveryState] = {
      reply match {
        case RunProtocol.Current(childView) =>
          val child = state.request.get.child
          DeliveryValidation.runView(childView, child, state.childIdentity, limits)
            .orElse(DeliveryValidation.runView(childView, child, state.childIdentity, state.policy.get.limits)) match {
            case Some(reason) => block(state, reason)
            case None => childView.status match {
              case RunProtocol.Status.Completed | RunProtocol.Status.Failed =>
                val failed = childView.status == RunProtocol.Status.Failed
                val reason = if (failed) Some(diagnostic(state, childView.reason.getOrElse("child run failed"))) else None
                commit(state, DeliveryEvent.ChildTerminalRecorded(childView.identity.get,
                  if (failed) Phase.Failed else Phase.Completed, childView.lastState.get, reason))(_ => owner.cancelTimers())
              case RunProtocol.Status.Running | RunProtocol.Status.AwaitingOperation =>
                val due = if (state.remainingPolls > 0) Some(Math.addExact(clock(), state.policy.get.pollIntervalMillis)) else None
                commit(state, DeliveryEvent.ChildRunning(childView.identity.get, due))(schedule)
              case RunProtocol.Status.NotStarted if call.kind == DeliveryState.Query && state.nextAction.contains(DeliveryState.Query) =>
                // Even after an earlier start ACK, never invent a new child ID.
                // StartOrAttach makes this exact frozen resubmission idempotent.
                ensureChild(state)
              case RunProtocol.Status.NotStarted => block(state, "child is not started; committed completion is unconfirmed")
              case RunProtocol.Status.RecoveryBlocked =>
                block(state, s"child recovery blocked: ${childView.reason.getOrElse("unavailable")}")
              case _ => block(state, "unknown child run status")
            }
          }
        case RunProtocol.Unavailable(_, reason) => block(state, "child unavailable: " + diagnostic(state, reason))
        case RunProtocol.Rejected(_, reason) => block(state, "child start/query rejected: " + diagnostic(state, reason))
        case null => block(state, "run port returned no reply; child outcome is unknown")
      }
    }

    val handler: (DeliveryState, Command) => Effect[DeliveryEvent, DeliveryState] = { (state, command) =>
      command match {
        case Deliver(request, replyTo) =>
          val invalid = DeliveryValidation.request(request, limits)
            .orElse(if (replyTo == null) Some("reply actor is required") else None)
            .orElse(if (request.deliveryId != deliveryId) Some("delivery id differs from this entity") else None)
          invalid match {
            case Some(reason) => reject(replyTo, deliveryId, reason); Effect.none
            case None if state.request.nonEmpty =>
              if (!state.request.contains(request) || !state.requestDigest.contains(request.fingerprint))
                reject(replyTo, request.deliveryId, "delivery id already has a different immutable request")
              else replyTo ! Current(view(state))
              Effect.none
            case None if problem(state).nonEmpty => replyTo ! Current(view(state)); Effect.none
            case None =>
              commit(state, DeliveryEvent.Accepted(request, request.fingerprint, settings.policy)) { next =>
                replyTo ! Current(view(next)) // Accepted must commit before this ACK.
                if (problem(next).isEmpty) self ! Drive(generation)
              }
          }

        case Get(key, replyTo) =>
          if (key != deliveryId || limits.text("delivery id", key).nonEmpty)
            reject(replyTo, key, "delivery id differs from this entity")
          else if (replyTo != null) replyTo ! Current(view(state))
          Effect.none

        case Reconcile(key, replyTo) =>
          if (key != deliveryId || limits.text("delivery id", key).nonEmpty) {
            reject(replyTo, key, "delivery id differs from this entity"); Effect.none
          } else if (replyTo == null) Effect.none
          else if (problem(state).nonEmpty || state.terminal) {
            replyTo ! Current(view(state)); Effect.none
          } else if (state.request.isEmpty) {
            reject(replyTo, key, "delivery has not been accepted"); Effect.none
          } else if (inFlight.nonEmpty) {
            reject(replyTo, key, "original child call is still outstanding"); Effect.none
          } else if (state.phase != Phase.Blocked || state.nextAction.nonEmpty) {
            replyTo ! Current(view(state)); Effect.none // Never replenish an active round.
          } else if (state.reconciliations >= state.policy.get.maxReconciliations) {
            reject(replyTo, key, "manual child reconciliation budget exhausted"); Effect.none
          } else commit(state, DeliveryEvent.Reconciled(state.reconciliations + 1, clock())) { next =>
            replyTo ! Current(view(next))
            if (problem(next).isEmpty) self ! Drive(generation)
          }

        case Drive(g) if fresh(g) => drive(state)
        case PollDue(g, token, due) if fresh(g) && token == timerToken && state.nextPollAt.contains(due) =>
          owner.cancelPoll()
          drive(state)

        case CallFinished(g, id, _) if g == generation && inFlight.exists(_.id == id) =>
          val call = inFlight.get
          // Read the real Future, not a notification payload. Only its actual
          // completion frees the slot, including after a watchdog timeout.
          call.future.value match {
            case None => Effect.none
            case Some(outcome) =>
              inFlight = None
              owner.cancelIO()
              if (!fresh(g) || problem(state).nonEmpty || state.terminal) Effect.none
              else outcome match {
                case Success(reply) => observed(state, call, reply)
                case Failure(_) => block(state, "child call failed; its outcome remains unknown")
              }
          }

        case CallTimedOut(g, id) if fresh(g) && inFlight.exists(_.id == id) && problem(state).isEmpty =>
          owner.cancelIO()
          inFlight.get.future.value match {
            case Some(outcome) => self ! CallFinished(g, id, outcome); Effect.none
            case None => block(state, "child I/O timed out; original call remains outstanding")
          }

        case _ => Effect.none // Stale owners, callback IDs and timer tokens are inert.
      }
    }

    val behavior = EventSourcedBehavior[Command, DeliveryEvent, DeliveryState](
      persistenceId, DeliveryState(), handler, DeliveryState.evolve).receiveSignal {
      case (state, RecoveryCompleted) =>
        if (problem(state).isEmpty && !state.terminal && state.nextAction.nonEmpty) self ! Drive(generation)
    }

    // Also clean up during replay or a pending journal ACK, not only when idle.
    Behaviors.intercept(() => new BehaviorSignalInterceptor[Command] {
      override def aroundSignal(context: TypedActorContext[Command], signal: Signal,
          target: BehaviorInterceptor.SignalTarget[Command]): Behavior[Command] = {
        if (signal == PostStop || signal == PreRestart) close()
        target(context, signal)
      }
    })(behavior)
  }
}
