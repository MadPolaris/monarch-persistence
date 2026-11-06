package net.imadz.monarch.persistence

import net.imadz.monarch.{CheckpointFailed, Monarch, RunLease, StageError, StaleRun}

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, BehaviorInterceptor, BehaviorSignalInterceptor, PostStop, PreRestart, Signal, TypedActorContext}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, Recovery, RetentionCriteria}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted, SnapshotFailed}
import akka.util.ByteString

import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** One durable run per entity. Legacy stage bodies must tolerate re-execution before
  * their checkpoint commits. managedBehavior binds external operations to that frontier.
  * Ownership is fenced only in this JVM; cross-node fencing is the host's responsibility.
  */
object PersistentMonarch {
  sealed trait Command[+S]
  trait PublicCommand extends Command[Nothing]
  final case class StartRun[S](runId: String, initialState: S, replyTo: ActorRef[RunReply]) extends Command[S]

  // Retained shapes for compatibility; unowned legacy callbacks are not accepted.
  private[persistence] final case class StageCompleted[S](cursor: String,
      metadata: Map[String, String], snapshot: S) extends Command[S]
  private[persistence] final case class RunSucceeded[S]() extends Command[S]
  private[persistence] final case class RunFailed[S](cursor: String, reason: String) extends Command[S]

  sealed trait Event
  final case class RunStarted(runId: String, stageCursors: Vector[String], initialSnapshot: Array[Byte]) extends Event
  final case class StageDone(cursor: String, timestamp: Long, metadata: Map[String, String], snapshot: Array[Byte]) extends Event
  final case class RunCompleted(runId: String) extends Event
  final case class RunFailedEvt(cursor: String, reason: String) extends Event
  final case class StageResolved(cursor: String, timestamp: Long, code: Option[String],
      errorCode: String, detail: String, snapshot: Array[Byte]) extends Event
  final case class RunPlanExtended(stageCursors: Vector[String]) extends Event
  final case class VersionedRunStarted(identity: RunProtocol.Identity, stageCursors: Vector[String],
      initialSnapshot: ByteString) extends Event
  final case class RunAwaiting(reference: RunProtocol.AwaitingRef) extends Event

  sealed trait RunState
  case object Idle extends RunState
  final case class Executing(runId: String, completedCursors: Vector[String], lastSnapshot: Array[Byte]) extends RunState
  final case class Done(runId: String) extends RunState
  final case class Failed(runId: String, cursor: String, reason: String) extends RunState

  sealed trait RunReply
  final case class Accepted(runId: String) extends RunReply
  final case class Rejected(runId: String, reason: String) extends RunReply

  private[persistence] final case class Checkpoint[S](generation: Long, ordinal: Int,
      cursor: String, metadata: Map[String, String], snapshot: S,
      resolution: Option[StageError], ack: Promise[Unit]) extends Command[S]
  private[persistence] final case class Diagnostic[S](generation: Long, cursor: String,
      error: StageError) extends Command[S]
  private[persistence] final case class Finished[S](generation: Long,
      failure: Option[(String, String)], infrastructure: Boolean = false) extends Command[S]
  private[persistence] final case class ResumeRun[S](generation: Long) extends Command[S]
  private final case class BindOperation(generation: Long, ordinal: Int,
      reference: RunProtocol.AwaitingRef, ack: Promise[Unit]) extends PublicCommand
  private final case class Paused(generation: Long, reason: String) extends PublicCommand

  /** Extra fields retain committed data without changing public Done/Failed constructors. */
  private[persistence] final case class DurableState(run: RunState,
      plan: Vector[String], invalid: Option[String], version: Int = 1,
      identity: Option[RunProtocol.Identity] = None,
      retainedSnapshot: Option[ByteString] = None,
      completedCursors: Vector[String] = Vector.empty,
      awaiting: Option[RunProtocol.AwaitingRef] = None)

  private[persistence] def evolve(state: DurableState, event: Event): DurableState = {
    def advance(cursor: String, bytes: Array[Byte]): DurableState = state.run match {
      case e: Executing =>
        val index = e.completedCursors.size
        val reason =
          if (index < state.plan.size && state.plan(index) != cursor)
            Some(s"non-contiguous checkpoint $cursor; expected ${state.plan(index)}")
          else if (index >= state.plan.size && e.completedCursors.contains(cursor)) Some(s"duplicate checkpoint $cursor")
          else None // Legacy appended checkpoints are validated against the host plan at recovery.
        val completed = e.completedCursors :+ cursor
        state.copy(run = e.copy(completedCursors = completed, lastSnapshot = bytes.clone()),
          retainedSnapshot = Some(ByteString.fromArray(bytes)), completedCursors = completed, awaiting = None,
          invalid = state.invalid.orElse(reason))
      case _ => state
    }
    event match {
      case RunStarted(id, plan, bytes) if state.run == Idle =>
        DurableState(Executing(id, Vector.empty, bytes.clone()), plan, None,
          retainedSnapshot = Some(ByteString.fromArray(bytes)))
      case VersionedRunStarted(identity, plan, bytes) if state.run == Idle =>
        val valid = identity.initialDigest == ProtocolDigest.sha256(bytes) &&
          identity.planDigest == ProtocolDigest.fields(plan.map(ByteString(_)))
        DurableState(Executing(identity.runId, Vector.empty, bytes.toArray), plan,
          if (valid) None else Some("recorded start identity mismatch"), version = 2,
          identity = Some(identity), retainedSnapshot = Some(bytes))
      case StageDone(cursor, _, _, bytes) => advance(cursor, bytes)
      case StageResolved(cursor, _, _, _, _, bytes) => advance(cursor, bytes)
      case RunAwaiting(reference) => state.run match {
        case e: Executing if state.identity.nonEmpty && state.plan.lift(e.completedCursors.size).contains(reference.cursor) &&
            state.awaiting.forall(_ == reference) => state.copy(awaiting = Some(reference))
        case _ => state.copy(invalid = state.invalid.orElse(Some("recorded operation reference mismatch")))
      }
      case RunPlanExtended(plan) if state.run.isInstanceOf[Executing] =>
        if (state.identity.isEmpty && plan.startsWith(state.plan)) state.copy(plan = plan)
        else state.copy(invalid = state.invalid.orElse(Some("recorded plan was rewritten")))
      case RunCompleted(id) => state.run match {
        case e: Executing => state.copy(run = Done(e.runId),
          invalid = state.invalid.orElse(if (id == e.runId) None else Some("terminal runId mismatch")))
        case _ => state
      }
      case RunFailedEvt(cursor, reason) => state.run match {
        case e: Executing => state.copy(run = Failed(e.runId, cursor, reason))
        case _ => state
      }
      case _ => state
    }
  }

  private[persistence] def recoveryProblem(state: DurableState, currentPlan: Vector[String]): Option[String] =
    state.invalid.orElse {
      if (state.version != 1 && state.version != 2) Some(s"unsupported snapshot version ${state.version}")
      else if (state.version == 2 && state.identity.isEmpty) Some("versioned run is missing its identity")
      else if (state.identity.nonEmpty && currentPlan != state.plan) Some("versioned host plan differs from the frozen plan")
      else if (!currentPlan.startsWith(state.plan)) Some("host plan is not an append-only extension of the recorded plan")
      else state.run match {
        case e: Executing if e.completedCursors != currentPlan.take(e.completedCursors.size) =>
          Some("checkpoint history is not a contiguous prefix of the host plan")
        case e: Executing if state.awaiting.exists(a => !currentPlan.lift(e.completedCursors.size).contains(a.cursor)) =>
          Some("operation reference is not at the committed frontier")
        case _ => None
      }
    }

  /** Existing signature retained. One in-flight checkpoint, finite timeout, no journal deletion. */
  def behavior[Stage, S](
      persistenceId: PersistenceId,
      stages: Seq[Stage],
      interpreter: (Stage, S) => Future[S],
      stateCodec: StateCodec[S],
      stageName: Stage => String = (s: Stage) => String.valueOf(s),
      failurePolicy: Option[(String, StageError, S) => Future[S]] = None,
      observer: RunObserver[S] = RunObserver.nop[S]
  )(implicit ec: ExecutionContext): Behavior[Command[S]] =
    behaviorWithSettings(persistenceId, stages, interpreter, stateCodec,
      PersistentMonarchSettings(), stageName, failurePolicy, observer)

  def behaviorWithSettings[Stage, S](
      persistenceId: PersistenceId,
      stages: Seq[Stage],
      interpreter: (Stage, S) => Future[S],
      stateCodec: StateCodec[S],
      settings: PersistentMonarchSettings,
      stageName: Stage => String = (s: Stage) => String.valueOf(s),
      failurePolicy: Option[(String, StageError, S) => Future[S]] = None,
      observer: RunObserver[S] = RunObserver.nop[S]
  )(implicit ec: ExecutionContext): Behavior[Command[S]] =
    buildBehavior(persistenceId, stages, (stage: Stage, state: S, _: Option[ManagedStageScope]) => interpreter(stage, state),
      stateCodec, settings, stageName, failurePolicy, observer, None, None, ProtocolLimits())

  def versionedBehavior[Stage, S](
      persistenceId: PersistenceId,
      definition: RunDefinition[Stage],
      interpreter: (Stage, S) => Future[S],
      stateCodec: StateCodec[S],
      settings: PersistentMonarchSettings = PersistentMonarchSettings(),
      limits: ProtocolLimits = ProtocolLimits(),
      observer: RunObserver[S] = RunObserver.nop[S]
  )(implicit ec: ExecutionContext): Behavior[Command[S]] =
    buildBehavior(persistenceId, definition.stages,
      (stage: Stage, state: S, _: Option[ManagedStageScope]) => interpreter(stage, state),
      stateCodec, settings, definition.stepId, None, observer, Some(definition), None, limits)

  def managedBehavior[Stage, S](
      persistenceId: PersistenceId,
      definition: RunDefinition[Stage],
      interpreter: (Stage, S, ManagedStageScope) => Future[S],
      stateCodec: StateCodec[S],
      operations: ManagedOperations,
      settings: PersistentMonarchSettings = PersistentMonarchSettings(),
      limits: ProtocolLimits = ProtocolLimits(),
      observer: RunObserver[S] = RunObserver.nop[S]
  )(implicit ec: ExecutionContext): Behavior[Command[S]] =
    buildBehavior(persistenceId, definition.stages,
      (stage: Stage, state: S, scope: Option[ManagedStageScope]) => interpreter(stage, state, scope.get),
      stateCodec, settings, definition.stepId, None, observer, Some(definition), Some(operations), limits)

  private def buildBehavior[Stage, S](
      persistenceId: PersistenceId, stages: Seq[Stage],
      interpreter: (Stage, S, Option[ManagedStageScope]) => Future[S],
      stateCodec: StateCodec[S], settings: PersistentMonarchSettings, stageName: Stage => String,
      failurePolicy: Option[(String, StageError, S) => Future[S]], observer: RunObserver[S],
      definition: Option[RunDefinition[Stage]], operations: Option[ManagedOperations], limits: ProtocolLimits
  )(implicit ec: ExecutionContext): Behavior[Command[S]] = Behaviors.setup { ctx =>
    val self = ctx.self
    implicit val scheduler: akka.actor.typed.Scheduler = ctx.system.scheduler
    val planStages = stages.toVector
    val stepNames = definition.map(_.stepIds).getOrElse(planStages.map(stageName))
    val plan = stepNames.zipWithIndex.map { case (name, index) => s"$name#$index" }
    var blocked: Option[String] = None

    def observe(body: => Unit): Unit =
      try body catch { case NonFatal(ex) => ctx.log.warn("Monarch observer failed for {}", persistenceId.id, ex) }

    final class Owner {
      val pending = new ConcurrentHashMap[Promise[Unit], java.lang.Boolean]()
      val scope = new AtomicReference[ManagedStageScope]()
      val lease: RunLease = RunLease.acquire(persistenceId.id, () => {
        Option(scope.getAndSet(null)).foreach(_.close())
        val iterator = pending.keySet().iterator()
        while (iterator.hasNext) iterator.next().tryFailure(StaleRun)
        pending.clear()
      })
      def close(): Unit = lease.revoke()
      def pause(reason: String): Unit = {
        close()
        self ! Paused(lease.generation, reason)
      }

      private def acknowledged(label: String, binding: Boolean)(message: Promise[Unit] => Command[S]): Future[Unit] = {
        val ack = Promise[Unit]()
        pending.put(ack, java.lang.Boolean.TRUE)
        if (!lease.isFresh) ack.tryFailure(StaleRun)
        val timeout = scheduler.scheduleOnce(settings.checkpointTimeout, new Runnable {
          override def run(): Unit = {
            val ex = new TimeoutException(s"$label timed out; commit outcome unknown")
            if (ack.tryFailure(StaleRun)) {
              // StaleRun bypasses classification even before the revocation callback executes.
              close()
              if (binding) self ! Paused(lease.generation, ex.getMessage)
              else self ! Finished[S](lease.generation, Some(label -> ex.getMessage), infrastructure = true)
            }
          }
        })(ctx.executionContext)
        ack.future.onComplete { _ => timeout.cancel(); pending.remove(ack); () }(ctx.executionContext)
        if (lease.isFresh && !ack.isCompleted) self ! message(ack)
        else ack.tryFailure(StaleRun)
        ack.future
      }

      def checkpoint(ordinal: Int, cursor: String, metadata: Map[String, String],
          state: S, error: Option[StageError]): Future[Unit] =
        acknowledged(s"checkpoint $cursor", binding = false)(ack =>
          Checkpoint(lease.generation, ordinal, cursor, metadata, state, error, ack))

      def bind(ordinal: Int, reference: RunProtocol.AwaitingRef): Future[Unit] =
        acknowledged(s"operation reference ${reference.cursor}", binding = true)(ack =>
          BindOperation(lease.generation, ordinal, reference, ack))
    }
    var owner: Option[Owner] = None
    def closeOwner(): Unit = { owner.foreach(_.close()); owner = None }
    def acquireOwner(): Owner = { closeOwner(); blocked = None; val next = new Owner; owner = Some(next); next }
    def current(generation: Long): Boolean = owner.exists(o => o.lease.generation == generation && o.lease.isFresh)

    def problem(state: DurableState): Option[String] = recoveryProblem(state, plan).orElse {
      state.identity.flatMap { id => definition match {
        case Some(d) if d.id == id.definitionId && d.version == id.definitionVersion &&
            d.codecVersion == id.codecVersion && d.planDigest == id.planDigest => None
        case _ => Some("host definition/codec version does not match the recorded identity")
      }}
    }.orElse {
      if (state.run != Idle && definition.nonEmpty && state.identity.isEmpty)
        Some("legacy run lacks the identity required by a versioned host") else None
    }.orElse(if (state.awaiting.nonEmpty && operations.isEmpty) Some("recorded operation requires a managed host") else None)
      .orElse(definition.flatMap(_.validate(limits)))

    def view(state: DurableState): RunProtocol.View = {
      import RunProtocol.Status
      val issue = blocked.orElse(problem(state)).orElse {
        if (state.run.isInstanceOf[Executing] && !owner.exists(_.lease.isFresh))
          Some("run owner unavailable; controlled recovery required") else None
      }
      val (id, status, cursors, bytes, failure) = state.run match {
        case Idle => (None, Status.NotStarted, Vector.empty[String], None, None)
        case e: Executing => (Some(e.runId), if (state.awaiting.nonEmpty) Status.AwaitingOperation else Status.Running,
          e.completedCursors, Some(ByteString.fromArray(e.lastSnapshot)), None)
        case Done(id) => (Some(id), Status.Completed, state.completedCursors, state.retainedSnapshot, None)
        case Failed(id, _, reason) => (Some(id), Status.Failed, state.completedCursors, state.retainedSnapshot, Some(reason))
      }
      RunProtocol.View(id, if (issue.nonEmpty) Status.RecoveryBlocked else status, cursors,
        bytes, state.identity, state.awaiting, issue.orElse(failure).map(limits.diagnostic))
    }

    def replyView(state: DurableState): RunProtocol.Reply = {
      val observed = view(state)
      RunProtocol.viewProblem(observed, limits) match {
        case Some(reason) => RunProtocol.Unavailable(observed.runId.map(limits.diagnostic),
          limits.diagnostic(s"committed run view is outside the wire limits: $reason"))
        case None => RunProtocol.Current(observed)
      }
    }

    def runEngine(state: S, completed: Int, runOwner: Owner): Unit = {
      val ordinal = new AtomicInteger(completed)
      def checkpoint(cursor: String, snapshot: S, metadata: Map[String, String], error: Option[StageError]): Future[Unit] = {
        val index = ordinal.get()
        runOwner.checkpoint(index, cursor, metadata, snapshot, error).map { _ =>
          ordinal.set(index + 1)
          Option(runOwner.scope.getAndSet(null)).foreach(_.close())
          ()
        }
      }
      def execute(stage: Stage, snapshot: S): Future[S] = {
        val index = ordinal.get()
        val stageScope = operations.map { runtime =>
          val scope = new ManagedStageScope(plan(index), definition.get.stepIds(index), runOwner.lease, runtime,
            ref => runOwner.bind(index, ref), reason => runOwner.pause(reason))(ctx.executionContext, scheduler)
          runOwner.scope.set(scope)
          if (!runOwner.lease.isFresh) scope.close()
          scope
        }
        val result = try interpreter(stage, snapshot, stageScope) catch { case NonFatal(ex) => Future.failed(ex) }
        result.transformWith { outcome =>
          if (stageScope.exists(_.hasUnconfirmedOperation)) {
            runOwner.pause("managed interpreter settled before its operation was confirmed")
            Future.failed(StaleRun)
          } else Future.fromTry(outcome)
        }
      }
      // Feed stable named entries to the unchanged core; naming functions are not re-evaluated after recovery.
      val entries = planStages.zip(stepNames)
      var builder = Monarch.builder[(Stage, String), S].namingStages(_._2)
        .withStages(entries).withExecution((entry, snapshot) => execute(entry._1, snapshot))
        .withGenerationGuard(() => runOwner.lease.isFresh)
        .onStageFailed { (cursor, error) => self ! Diagnostic[S](runOwner.lease.generation, cursor, error) }
        .withCheckpoints(
          (cursor, snapshot, metadata) => checkpoint(cursor, snapshot, metadata, None),
          (cursor, error, snapshot) => checkpoint(cursor, snapshot, Map.empty, Some(error)))
      failurePolicy.foreach(policy => builder = builder.onFailure(policy))
      val result = try builder.build().resumeFromIndex(state, completed)
        catch { case NonFatal(ex) => Future.failed(ex) }
      result.onComplete {
        case Success(_) => self ! Finished[S](runOwner.lease.generation, None)
        case Failure(StaleRun) => ()
        case Failure(CheckpointFailed(cursor, cause)) =>
          runOwner.close()
          self ! Finished[S](runOwner.lease.generation, Some(cursor -> Option(cause.getMessage).getOrElse(cause.toString)), infrastructure = true)
        case Failure(ex) =>
          self ! Finished[S](runOwner.lease.generation, Some("pipeline" -> Option(ex.getMessage).getOrElse(ex.toString)))
      }
    }

    def encode(state: S): Either[String, Array[Byte]] = try {
      val bytes = stateCodec.encode(state)
      if (bytes == null) Left("state codec returned null")
      else if (bytes.length > limits.maxStateBytes) Left("state exceeds protocol size limit")
      else Right(bytes.clone())
    } catch { case NonFatal(ex) => Left(s"state encode failed: ${ex.getMessage}") }

    val handler: (DurableState, Command[S]) => Effect[Event, DurableState] = { (state, command) =>
      command match {
        case RunProtocol.GetRun(replyTo) => replyTo ! replyView(state); Effect.none
        case request: RunProtocol.StartOrAttach =>
          val issue = limits.text("runId", request.runId)
            .orElse(limits.bytes("initialState", request.initialState, limits.maxStateBytes))
            .orElse(definition.flatMap(_.validate(limits)))
            .orElse(definition match {
              case Some(d) if d.id == request.definitionId && d.version == request.definitionVersion &&
                  d.codecVersion == request.codecVersion => None
              case _ => Some("use a matching versioned or managed run definition")
            })
          issue match {
            case Some(reason) => request.replyTo ! RunProtocol.Rejected(limits.diagnostic(request.runId), limits.diagnostic(reason)); Effect.none
            case None =>
              val identity = definition.get.identity(request.runId, request.initialState)
              if (state.run == Idle) {
                val decoded = try Right(stateCodec.decode(request.initialState.toArray)) catch { case NonFatal(ex) => Left(ex) }
                decoded match {
                  case Left(ex) => request.replyTo ! RunProtocol.Rejected(request.runId,
                    limits.diagnostic(s"initial state decode failed: ${ex.getMessage}")); Effect.none
                  case Right(initial) => Effect.persist(VersionedRunStarted(identity, plan, request.initialState)).thenRun { committed =>
                    val next = acquireOwner()
                    request.replyTo ! replyView(committed)
                    observe(observer.onStart(request.runId))
                    runEngine(initial, 0, next)
                  }
                }
              } else if (state.identity.contains(identity)) {
                request.replyTo ! replyView(state)
                Effect.none // Attaching is an observation, never another runEngine invocation.
              } else {
                request.replyTo ! RunProtocol.Rejected(request.runId, "run identity/request conflict or legacy identity unavailable")
                Effect.none
              }
          }

        case StartRun(id, _, replyTo) if definition.nonEmpty =>
          replyTo ! Rejected(id, "versioned/managed runs require StartOrAttach")
          Effect.none
        case StartRun(id, initial, replyTo) if state.run == Idle =>
          encode(initial) match {
            case Left(reason) => replyTo ! Rejected(id, s"initial $reason"); Effect.none
            case Right(bytes) => Effect.persist(RunStarted(id, plan, bytes)).thenRun { (_: DurableState) =>
              val next = acquireOwner()
              observe(observer.onStart(id))
              replyTo ! Accepted(id)
              runEngine(initial, 0, next)
            }
          }
        case StartRun(id, _, replyTo) =>
          replyTo ! Rejected(id, s"run already in state ${state.run.getClass.getSimpleName}")
          Effect.none

        case bind: BindOperation => state.run match {
          case e: Executing if current(bind.generation) && !bind.ack.isCompleted && owner.exists(_.pending.containsKey(bind.ack)) =>
            val issue = if (state.invalid.nonEmpty || state.identity.isEmpty || bind.ordinal != e.completedCursors.size ||
                plan.lift(bind.ordinal) != Some(bind.reference.cursor)) Some("operation reference/frontier mismatch")
              else limits.text("operationKey", bind.reference.operationKey)
                .orElse(limits.text("requestDigest", bind.reference.requestDigest))
                .orElse(if (state.awaiting.exists(_ != bind.reference)) Some("operation reference conflicts with frozen request") else None)
            issue match {
              case Some(reason) =>
                owner.foreach(_.pause(reason)); bind.ack.tryFailure(StaleRun); Effect.none
              case None if state.awaiting.contains(bind.reference) => bind.ack.trySuccess(()); Effect.none
              case None => Effect.persist(RunAwaiting(bind.reference)).thenRun { (_: DurableState) =>
                if (current(bind.generation)) bind.ack.trySuccess(()) else bind.ack.tryFailure(StaleRun)
                ()
              }
            }
          case _ => bind.ack.tryFailure(StaleRun); Effect.none
        }

        case checkpoint: Checkpoint[S] => state.run match {
          case e: Executing if current(checkpoint.generation) && !checkpoint.ack.isCompleted &&
              owner.exists(_.pending.containsKey(checkpoint.ack)) =>
            val confirmed = state.awaiting.forall { reference =>
              owner.flatMap(o => Option(o.scope.get())).exists(_.confirms(reference))
            }
            if (state.invalid.nonEmpty || checkpoint.ordinal != e.completedCursors.size ||
                plan.lift(checkpoint.ordinal) != Some(checkpoint.cursor) ||
                state.plan.size != plan.size || state.plan.lift(checkpoint.ordinal) != Some(checkpoint.cursor) || !confirmed) {
              if (definition.nonEmpty) {
                owner.foreach(_.pause("checkpoint identity/frontier or operation confirmation mismatch"))
                checkpoint.ack.tryFailure(StaleRun)
                Effect.none
              } else {
                checkpoint.ack.tryFailure(new IllegalStateException("checkpoint identity/frontier mismatch"))
                closeOwner()
                Effect.stop()
              }
            } else {
              val encoded = limits.metadata(checkpoint.metadata).toLeft(()).flatMap(_ => encode(checkpoint.snapshot))
              encoded match {
                case Left(reason) =>
                  ctx.log.error("Monarch checkpoint encode failed for {}: {}", persistenceId.id, reason)
                  closeOwner()
                  checkpoint.ack.tryFailure(StaleRun)
                  Effect.stop()
                case Right(bytes) if definition.nonEmpty &&
                    limits.bytes("snapshot envelope", ByteString.fromArray(MonarchProtoConverters.snapshotToProto(
                      state.copy(run = e.copy(completedCursors = e.completedCursors :+ checkpoint.cursor, lastSnapshot = bytes),
                        retainedSnapshot = Some(ByteString.fromArray(bytes)),
                        completedCursors = e.completedCursors :+ checkpoint.cursor, awaiting = None)).toByteArray),
                      limits.maxEnvelopeBytes).nonEmpty =>
                  owner.foreach(_.pause("checkpoint snapshot exceeds envelope limit"))
                  checkpoint.ack.tryFailure(StaleRun)
                  Effect.none
                case Right(bytes) =>
                  checkpoint.resolution match {
                    case None => observe(observer.onStageComplete(checkpoint.cursor, checkpoint.metadata, checkpoint.snapshot))
                    case _ => ()
                  }
                  val event: Event = checkpoint.resolution match {
                    case Some(error) => StageResolved(checkpoint.cursor, System.currentTimeMillis(), error.code, error.errorCode, error.detail, bytes)
                    case None => StageDone(checkpoint.cursor, System.currentTimeMillis(), checkpoint.metadata, bytes)
                  }
                  Effect.persist(event).thenRun { (_: DurableState) =>
                    if (current(checkpoint.generation) && !checkpoint.ack.isCompleted) {
                      observer match {
                        case durable: DurableRunObserver[S] => checkpoint.resolution match {
                          case Some(error) => observe(durable.onResolutionCommitted(checkpoint.cursor, error, checkpoint.snapshot))
                          case None => observe(durable.onCheckpointCommitted(checkpoint.cursor, checkpoint.metadata, checkpoint.snapshot))
                        }
                        case _ => ()
                      }
                      checkpoint.ack.trySuccess(())
                    } else checkpoint.ack.tryFailure(StaleRun)
                    ()
                  }
              }
            }
          case _ => checkpoint.ack.tryFailure(StaleRun); Effect.none
        }

        case Paused(generation, reason) if owner.exists(_.lease.generation == generation) =>
          blocked = Some(reason)
          closeOwner()
          Effect.none
        case Diagnostic(generation, cursor, error) if current(generation) && state.run.isInstanceOf[Executing] =>
          observe(observer.onStageFailed(cursor, error.detail))
          ctx.log.warn("Monarch {} stage {} failed: {}", persistenceId.id, cursor, error.detail)
          Effect.none
        case Finished(generation, failure, true) if owner.exists(_.lease.generation == generation) =>
          ctx.log.error("Monarch {} checkpoint failed; controlled recovery required: {}", persistenceId.id, failure)
          closeOwner()
          Effect.stop()
        case Finished(generation, failure, false) if current(generation) => state.run match {
          case e: Executing => failure match {
            case None if e.completedCursors == plan && state.invalid.isEmpty && state.awaiting.isEmpty =>
              Effect.persist(RunCompleted(e.runId)).thenRun { (_: DurableState) =>
                closeOwner()
                observe(observer.onCompleted(e.runId))
              }
            case Some((cursor, reason)) if state.awaiting.isEmpty || owner.flatMap(o => Option(o.scope.get())).exists(s =>
                state.awaiting.forall(s.confirms)) =>
              Effect.persist(RunFailedEvt(cursor, limits.diagnostic(reason))).thenRun { (_: DurableState) =>
                closeOwner()
                observe(observer.onFailed(e.runId, cursor, reason))
              }
            case _ => owner.foreach(_.pause("run settled without operation confirmation or complete frontier")); Effect.none
          }
          case _ => Effect.none
        }

        case ResumeRun(generation) if current(generation) => state.run match {
          case _: Executing if state.plan != plan =>
            Effect.persist(RunPlanExtended(plan)).thenRun { (_: DurableState) => self ! ResumeRun[S](generation) }
          case e: Executing =>
            val decoded = try Right(stateCodec.decode(e.lastSnapshot.clone())) catch { case NonFatal(ex) => Left(ex) }
            decoded match {
              case Right(snapshot) => runEngine(snapshot, e.completedCursors.size, owner.get); Effect.none
              case Left(ex) =>
                ctx.log.error("Monarch {} recovery decode failed", persistenceId.id, ex)
                closeOwner()
                if (definition.nonEmpty) { blocked = Some("last checkpoint cannot be decoded by the pinned codec"); Effect.none }
                else Effect.stop()
            }
          case _ => Effect.none
        }
        case _ => Effect.none
      }
    }

    val base = EventSourcedBehavior[Command[S], Event, DurableState](persistenceId,
      DurableState(Idle, Vector.empty, None), handler, evolve).withRecovery(Recovery.default)
    val configured = settings.snapshotEvery.fold(base) { count =>
      base.withRetention(RetentionCriteria.snapshotEvery(count, settings.keepSnapshots))
    }
    val behavior = configured.receiveSignal {
      case (state, RecoveryCompleted) =>
        blocked = problem(state)
        state.run match {
          case e: Executing if blocked.isEmpty =>
            val next = acquireOwner()
            observe(observer.onRecovery(e.runId, e.completedCursors.size))
            self ! ResumeRun[S](next.lease.generation)
          case _: Executing =>
            ctx.log.error("Monarch {} recovery rejected; manual recovery required: {}", persistenceId.id, blocked)
            closeOwner()
          case _ => ()
        }
      case (_, failure: SnapshotFailed) => ctx.log.warn("Monarch {} snapshot save failed", persistenceId.id, failure.failure)
    }

    Behaviors.intercept(() => new BehaviorSignalInterceptor[Command[S]] {
      override def aroundSignal(context: TypedActorContext[Command[S]], signal: Signal,
          target: BehaviorInterceptor.SignalTarget[Command[S]]): Behavior[Command[S]] = {
        if (signal == PreRestart || signal == PostStop) closeOwner()
        target(context, signal)
      }
    })(behavior)
  }

  def initSharding[Stage, S](sharding: ClusterSharding, typeKeyName: String,
      behaviorFor: String => Behavior[Command[S]]): EntityTypeKey[Command[S]] = {
    val key = EntityTypeKey[Command[S]](typeKeyName)
    sharding.init(Entity(key) { entityCtx => behaviorFor(entityCtx.entityId) })
    key
  }
}
