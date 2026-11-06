package net.imadz.monarch.persistence.operation

import akka.actor.Cancellable
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, BehaviorInterceptor, BehaviorSignalInterceptor, PostStop, PreRestart, Signal, TypedActorContext}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted}
import net.imadz.monarch.{RunLease, StaleRun}
import net.imadz.monarch.persistence.operation.OperationProtocol._

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** One durable external operation, not a stage engine.
  *
  * Prepared commits before acceptance; DispatchStarted commits before submit.
  * Recovery of a possible dispatch always queries the exact recorded Request.
  * Only a validated query proof can authorize bounded same-identity resubmission.
  * Get observes committed state; a hung journal may also delay Get/Ensure.
  *
  * A timeout DOES NOT cancel a Future or an external effect. It persists Blocked
  * and retains the live-call slot until that original Future really completes.
  * Reconcile cannot bypass this hold. After controlled actor recreation, an
  * explicit Reconcile can query again; recreation does not cancel old external
  * work. Hosts must not repeatedly recreate actors to accumulate uncancellable
  * calls. At most one logical external call is live per active owner.
  *
  * RunLease fencing is JVM-local, namespaced separately from PersistentMonarch.
  * Reliable journals and a single effective writer remain host obligations;
  * lease revocation cannot retract an already-issued external effect or write.
  */
object PersistentOperation {
  private sealed trait CallKind
  private case object SubmitCall extends CallKind
  private case object QueryCall extends CallKind
  private final case class LiveCall(id: Long, kind: CallKind, future: Future[Observation])

  private[persistence] def leaseKey(id: PersistenceId): String = s"monarch:operation:${id.id}"

  def behavior(
      persistenceId: PersistenceId,
      operationKey: String,
      port: ExternalOperationPort,
      settings: OperationSettings = OperationSettings()
  )(implicit ec: ExecutionContext): Behavior[Command] =
    behaviorWithClock(persistenceId, operationKey, port, settings, () => System.currentTimeMillis())

  // Tests pair this clock with ManualTime; business code uses the public factory.
  private[persistence] def behaviorWithClock(
      persistenceId: PersistenceId,
      operationKey: String,
      port: ExternalOperationPort,
      settings: OperationSettings,
      clock: () => Long
  )(implicit ec: ExecutionContext): Behavior[Command] = Behaviors.setup { ctx =>
    require(port != null && settings != null, "operation port and settings are required")
    val limits = settings.limits
    OperationValidation.requireValid(limits.text("operation key", operationKey))
    val connectorId = port.connectorId
    val connectorVersion = port.connectorVersion
    OperationValidation.requireValid(limits.text("connector id", connectorId))
    OperationValidation.requireValid(limits.text("connector version", connectorVersion))
    val self = ctx.self
    val scheduler = ctx.system.scheduler

    final class Owner {
      private val ioTimer = new AtomicReference[Cancellable]()
      private val queryTimer = new AtomicReference[Cancellable]()
      val lease: RunLease = RunLease.acquire(leaseKey(persistenceId), () => cancelTimers())

      private def cancel(slot: AtomicReference[Cancellable]): Unit =
        Option(slot.getAndSet(null)).foreach(_.cancel())
      private def install(slot: AtomicReference[Cancellable], task: Cancellable): Unit = {
        Option(slot.getAndSet(task)).foreach(_.cancel())
        if (!lease.isFresh && slot.compareAndSet(task, null)) task.cancel()
      }
      def setIO(task: Cancellable): Unit = install(ioTimer, task)
      def setQuery(task: Cancellable): Unit = install(queryTimer, task)
      def cancelIO(): Unit = cancel(ioTimer)
      def cancelQuery(): Unit = cancel(queryTimer)
      def cancelTimers(): Unit = { cancelIO(); cancelQuery() }
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
    def problem(state: OperationState): Option[String] =
      localProblem.orElse(if (owner.lease.isFresh) None else Some("operation owner has been revoked"))
        .orElse(OperationState.recoveryProblem(state, operationKey, connectorId, connectorVersion, limits))

    def view(state: OperationState): View = {
      val blocked = problem(state)
      View(operationKey, state.requestDigest, if (blocked.nonEmpty) Phase.RecoveryBlocked else state.phase,
        if (blocked.nonEmpty) None else state.terminal, blocked.orElse(state.reason),
        if (blocked.nonEmpty) None else state.nextCheckAt, state.checks)
    }

    def bounded(message: String, fallback: String): String =
      if (limits.text("reason", message).isEmpty) message else fallback

    def reject(replyTo: ActorRef[Reply], key: String, reason: String): Unit =
      if (replyTo != null) replyTo ! Rejected(
        if (limits.text("operation key", key).isEmpty) key else operationKey,
        bounded(reason, "invalid operation request"))

    def commit(state: OperationState, event: OperationEvent)
        (after: OperationState => Unit): Effect[OperationEvent, OperationState] = {
      val candidate = OperationState.evolve(state, event)
      val invalid = OperationValidation.event(event, limits)
        .orElse(candidate.invalid).orElse(OperationValidation.state(candidate, limits))
      invalid match {
        case Some(reason) =>
          // An internal frontier/configuration error is not a business failure.
          localProblem = Some(bounded(reason, "invalid operation transition"))
          owner.cancelTimers()
          ctx.log.error("Operation {} blocked before an invalid journal transition: {}", operationKey, reason)
          Effect.none
        case None => Effect.persist(event).thenRun(after)
      }
    }

    def startCall(state: OperationState, kind: CallKind): Unit = {
      if (problem(state).isEmpty && inFlight.isEmpty && state.terminal.isEmpty) {
        owner.cancelQuery()
        val request = state.request.get
        val previous = state.lastEvidence
        callSequence += 1L
        val id = callSequence
        val lease = owner.lease
        // Flattening also bounds a connector that blocks before returning its
        // Future: its logical call remains outstanding after our timeout.
        val future: Future[Observation] = try {
          Future {
            if (!lease.isFresh) Future.failed[Observation](StaleRun)
            else kind match {
              case SubmitCall => port.submit(request)
              case QueryCall => port.query(request, previous)
            }
          }.flatMap { result =>
            if (result == null) Future.failed(new IllegalStateException("connector returned a null Future"))
            else result
          }(ctx.executionContext)
        } catch { case NonFatal(ex) => Future.failed(ex) }
        inFlight = Some(LiveCall(id, kind, future))
        owner.setIO(scheduler.scheduleOnce(state.policy.get.ioTimeoutMillis.millis, new Runnable {
          override def run(): Unit = self ! CallTimedOut(generation, id)
        })(ctx.executionContext))
        future.onComplete(outcome => self ! CallFinished(generation, id, outcome))(ctx.executionContext)
      }
    }

    def schedule(state: OperationState): Unit = {
      if (problem(state).isEmpty && inFlight.isEmpty && state.terminal.isEmpty) {
        state.nextCheckAt.foreach { due =>
          timerToken += 1L
          val token = timerToken
          val delay = math.max(0L, math.min(OperationSettings.MaxDelay.toMillis, due - clock())).millis
          owner.setQuery(scheduler.scheduleOnce(delay, new Runnable {
            override def run(): Unit = self ! CheckDue(generation, token, due)
          })(ctx.executionContext))
        }
      }
    }

    def block(state: OperationState, reason: String): Effect[OperationEvent, OperationState] = {
      owner.cancelQuery()
      commit(state, OperationEvent.Blocked(bounded(reason, "invalid external observation")))(_ => ())
    }

    def nextDue(state: OperationState): Option[Long] =
      if (state.remainingChecks <= 0) None
      else Some(Math.addExact(clock(), state.policy.get.checkIntervalMillis))

    def unknown(state: OperationState, reason: String): Effect[OperationEvent, OperationState] = {
      val next = nextDue(state)
      val message = if (next.isEmpty) OperationState.Exhausted
        else bounded(reason, "external outcome remains unknown")
      commit(state, OperationEvent.UnknownObserved(message, next))(schedule)
    }

    def connectorProblem(check: => Either[String, Unit]): Option[String] = try {
      check match {
        case Right(()) => None
        case Left(reason) => Some(bounded(reason, "connector rejected external proof"))
        case _ => Some("connector returned invalid proof validation")
      }
    } catch { case NonFatal(_) => Some("connector proof validation failed") }

    def observed(state: OperationState, call: LiveCall, observation: Observation)
        : Effect[OperationEvent, OperationState] = observation match {
      case withProof: WithEvidence =>
        val request = state.request.get
        OperationValidation.observation(withProof, request, state.lastEvidence, limits)
          .orElse(connectorProblem(port.validateEvidence(request, withProof))) match {
          case Some(reason) => block(state, reason)
          case None => withProof match {
            case Confirmed(terminal) =>
              commit(state, OperationEvent.TerminalRecorded(terminal))(_ => owner.cancelTimers())
            case Accepted(evidence) =>
              commit(state, OperationEvent.RunningObserved(evidence, nextDue(state)))(schedule)
            case Running(evidence) =>
              commit(state, OperationEvent.RunningObserved(evidence, nextDue(state)))(schedule)
            case SafeToSubmit(evidence) if call.kind == QueryCall =>
              connectorProblem(port.validateSafeToSubmit(request, evidence, state.lastEvidence)) match {
                case Some(reason) => block(state, reason)
                case None if state.submissions >= state.policy.get.maxSubmissions =>
                  block(state, "submission budget exhausted; no resubmission performed")
                case None =>
                  commit(state, OperationEvent.DispatchStarted(state.submissions + 1, Some(evidence)))(
                    next => startCall(next, SubmitCall))
              }
            case _: SafeToSubmit => block(state, "only a query can authorize safe resubmission")
          }
        }
      case Unknown(reason) => unknown(state, reason)
      case NotFound(reason) => unknown(state, reason)
      case Idle(reason) => unknown(state, reason)
      case null => unknown(state, "connector returned no observation")
    }

    def drive(state: OperationState): Effect[OperationEvent, OperationState] = {
      if (problem(state).nonEmpty || inFlight.nonEmpty || state.terminal.nonEmpty) Effect.none
      else state.phase match {
        case Phase.Empty | Phase.Blocked => Effect.none
        case Phase.Prepared =>
          commit(state, OperationEvent.DispatchStarted(1, None))(next => startCall(next, SubmitCall))
        case _ if state.nextCheckAt.exists(_ > clock()) => schedule(state); Effect.none
        case _ if state.remainingChecks <= 0 => block(state, OperationState.Exhausted)
        case _ =>
          commit(state, OperationEvent.QueryStarted(state.checks + 1))(next => startCall(next, QueryCall))
      }
    }

    val handler: (OperationState, Command) => Effect[OperationEvent, OperationState] = { (state, command) =>
      command match {
        case Ensure(request, replyTo) =>
          val invalid = OperationValidation.request(request, limits)
            .orElse(if (replyTo == null) Some("reply actor is required") else None)
            .orElse(if (request.operationKey != operationKey) Some("operation key differs from this entity") else None)
          invalid match {
            case Some(reason) => reject(replyTo, operationKey, reason); Effect.none
            case None if state.request.nonEmpty =>
              if (!state.request.contains(request) || !state.requestDigest.contains(request.fingerprint))
                reject(replyTo, request.operationKey, "operation key already has a different immutable request")
              else replyTo ! Current(view(state))
              Effect.none
            case None if problem(state).nonEmpty => replyTo ! Current(view(state)); Effect.none
            case None if request.connectorId != connectorId || request.connectorVersion != connectorVersion =>
              reject(replyTo, request.operationKey, "request connector id or version is not registered")
              Effect.none
            case None =>
              commit(state, OperationEvent.Prepared(request, request.fingerprint, settings.policy)) { next =>
                replyTo ! Current(view(next)) // Never acknowledge an uncommitted intent.
                if (problem(next).isEmpty) self ! Drive(generation)
              }
          }

        case Get(key, replyTo) =>
          if (key != operationKey || limits.text("operation key", key).nonEmpty)
            reject(replyTo, key, "operation key differs from this entity")
          else if (replyTo != null) replyTo ! Current(view(state))
          Effect.none

        case Reconcile(key, replyTo) =>
          if (key != operationKey || limits.text("operation key", key).nonEmpty) {
            reject(replyTo, key, "operation key differs from this entity"); Effect.none
          } else if (replyTo == null) Effect.none
          else if (problem(state).nonEmpty || state.terminal.nonEmpty) {
            replyTo ! Current(view(state)); Effect.none
          } else if (state.request.isEmpty) {
            reject(replyTo, key, "operation has not been prepared"); Effect.none
          } else if (inFlight.nonEmpty) {
            reject(replyTo, key, "original external call is still outstanding"); Effect.none
          } else if (state.phase != Phase.Blocked) {
            replyTo ! Current(view(state)); Effect.none // Do not replenish an active grant.
          } else if (state.reconciliations >= state.policy.get.maxReconciliations) {
            reject(replyTo, key, "manual reconciliation budget exhausted"); Effect.none
          } else {
            val policy = state.policy.get
            commit(state, OperationEvent.Reconciled(state.reconciliations + 1, policy.manualCheckBudget, clock())) { next =>
              replyTo ! Current(view(next))
              if (problem(next).isEmpty) self ! Drive(generation)
            }
          }

        case Drive(g) if fresh(g) => drive(state)
        case CheckDue(g, token, due) if fresh(g) && token == timerToken && state.nextCheckAt.contains(due) =>
          owner.cancelQuery()
          drive(state)

        case CallFinished(g, id, outcome) if g == generation && inFlight.exists(_.id == id) =>
          val call = inFlight.get
          inFlight = None // Free the slot only on actual completion, not timeout.
          owner.cancelIO()
          if (!fresh(g) || problem(state).nonEmpty || state.terminal.nonEmpty) Effect.none
          else outcome match {
            case Success(observation) => observed(state, call, observation)
            case Failure(_) => unknown(state, "external call failed; its outcome is unknown")
          }

        case CallTimedOut(g, id) if fresh(g) && inFlight.exists(_.id == id) && problem(state).isEmpty =>
          owner.cancelIO()
          inFlight.get.future.value match {
            case Some(result) => self ! CallFinished(g, id, result); Effect.none
            case None => block(state, "external I/O timed out; original call remains outstanding")
          }

        case _ => Effect.none // Late generations, call ids and timer tokens are inert.
      }
    }

    val behavior = EventSourcedBehavior[Command, OperationEvent, OperationState](
      persistenceId, OperationState(), handler, OperationState.evolve).receiveSignal {
      case (state, RecoveryCompleted) =>
        if (problem(state).isEmpty && state.request.nonEmpty && state.terminal.isEmpty && state.phase != Phase.Blocked)
          self ! Drive(generation)
    }

    // Intercept lifecycle even while the ESB is persisting or replaying.
    Behaviors.intercept(() => new BehaviorSignalInterceptor[Command] {
      override def aroundSignal(context: TypedActorContext[Command], signal: Signal,
          target: BehaviorInterceptor.SignalTarget[Command]): Behavior[Command] = {
        if (signal == PostStop || signal == PreRestart) close()
        target(context, signal)
      }
    })(behavior)
  }
}
