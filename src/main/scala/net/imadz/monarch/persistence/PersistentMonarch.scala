package net.imadz.monarch.persistence

import net.imadz.monarch.{CheckpointFailed, Monarch, RunLease, StageError, StaleRun}

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, BehaviorInterceptor, BehaviorSignalInterceptor, PostStop, PreRestart, Signal, TypedActorContext}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, Recovery, RetentionCriteria}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted, SnapshotFailed}

import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** One durable run per entity. Stage bodies and failure policies must tolerate
  * re-execution before their checkpoint commits. Ownership is fenced only in this
  * JVM; external effects and cross-node fencing are the host's responsibility.
  */
object PersistentMonarch {
  sealed trait Command[S]
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

  /** Pure replay state, including the plan discarded by the old wrapper. */
  private[persistence] final case class DurableState(run: RunState,
      plan: Vector[String], invalid: Option[String], version: Int = 1)

  private[persistence] def evolve(state: DurableState, event: Event): DurableState = {
    def advance(cursor: String, bytes: Array[Byte]): DurableState = state.run match {
      case e: Executing =>
        val index = e.completedCursors.size
        val reason =
          if (index < state.plan.size && state.plan(index) != cursor)
            Some(s"non-contiguous checkpoint $cursor; expected ${state.plan(index)}")
          else if (index >= state.plan.size && e.completedCursors.contains(cursor)) Some(s"duplicate checkpoint $cursor")
          else None // Legacy appended checkpoints are validated against the host plan at recovery.
        state.copy(run = e.copy(completedCursors = e.completedCursors :+ cursor, lastSnapshot = bytes),
          invalid = state.invalid.orElse(reason))
      case _ => state
    }
    event match {
      case RunStarted(id, plan, bytes) if state.run == Idle =>
        DurableState(Executing(id, Vector.empty, bytes), plan, None)
      case StageDone(cursor, _, _, bytes) => advance(cursor, bytes)
      case StageResolved(cursor, _, _, _, _, bytes) => advance(cursor, bytes)
      case RunPlanExtended(plan) if state.run.isInstanceOf[Executing] =>
        if (plan.startsWith(state.plan)) state.copy(plan = plan)
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
      if (state.version != 1) Some(s"unsupported snapshot version ${state.version}")
      else if (!currentPlan.startsWith(state.plan)) Some("host plan is not an append-only extension of the recorded plan")
      else state.run match {
        case e: Executing if e.completedCursors != currentPlan.take(e.completedCursors.size) =>
          Some("checkpoint history is not a contiguous prefix of the host plan")
        case _ => None
      }
    }

  /** Existing signature retained. Defaults: one in-flight checkpoint, finite
    * timeout, snapshots disabled and no journal deletion.
    */
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
  )(implicit ec: ExecutionContext): Behavior[Command[S]] = Behaviors.setup { ctx =>
    val self = ctx.self
    val scheduler = ctx.system.scheduler
    val planStages = stages.toVector
    val plan = planStages.iterator.zipWithIndex.map { case (stage, index) => s"${stageName(stage)}#$index" }.toVector

    def observe(body: => Unit): Unit =
      try body catch { case NonFatal(ex) => ctx.log.warn("Monarch observer failed for {}", persistenceId.id, ex) }

    final class Owner {
      val pending = new ConcurrentHashMap[Promise[Unit], java.lang.Boolean]()
      val lease: RunLease = RunLease.acquire(persistenceId.id, () => {
        val iterator = pending.keySet().iterator()
        while (iterator.hasNext) iterator.next().tryFailure(StaleRun)
        pending.clear()
      })
      def close(): Unit = lease.revoke()

      def checkpoint(ordinal: Int, cursor: String, metadata: Map[String, String],
          state: S, error: Option[StageError]): Future[Unit] = {
        val ack = Promise[Unit]()
        pending.put(ack, java.lang.Boolean.TRUE)
        if (!lease.isFresh) ack.tryFailure(StaleRun)
        val timeout = scheduler.scheduleOnce(settings.checkpointTimeout, new Runnable {
          override def run(): Unit = {
            val ex = new TimeoutException(s"checkpoint $cursor timed out; commit outcome unknown")
            if (ack.tryFailure(ex)) {
              close()
              self ! Finished[S](lease.generation, Some(cursor -> ex.getMessage), infrastructure = true)
            }
          }
        })
        ack.future.onComplete { _ => timeout.cancel(); pending.remove(ack); () }
        if (lease.isFresh && !ack.isCompleted)
          self ! Checkpoint(lease.generation, ordinal, cursor, metadata, state, error, ack)
        else ack.tryFailure(StaleRun)
        ack.future
      }
    }
    var owner: Option[Owner] = None
    def closeOwner(): Unit = { owner.foreach(_.close()); owner = None }
    def acquireOwner(): Owner = { closeOwner(); val next = new Owner; owner = Some(next); next }
    def current(generation: Long): Boolean = owner.exists(o => o.lease.generation == generation && o.lease.isFresh)

    def runEngine(state: S, completed: Int, runOwner: Owner): Unit = {
      var ordinal = completed
      def checkpoint(cursor: String, snapshot: S, metadata: Map[String, String], error: Option[StageError]): Future[Unit] = {
        val index = ordinal
        runOwner.checkpoint(index, cursor, metadata, snapshot, error).map { _ => ordinal = index + 1; () }
      }
      var builder = Monarch.builder[Stage, S].namingStages(stageName)
        .withStages(planStages).withExecution(interpreter)
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

    val handler: (DurableState, Command[S]) => Effect[Event, DurableState] = { (state, command) =>
      command match {
        case StartRun(id, initial, replyTo) if state.run == Idle =>
          val encoded = try Right(stateCodec.encode(initial)) catch { case NonFatal(ex) => Left(ex) }
          encoded match {
            case Left(ex) =>
              replyTo ! Rejected(id, s"initial state encode failed: ${ex.getMessage}")
              Effect.none
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

        case checkpoint: Checkpoint[S] => state.run match {
          case e: Executing if current(checkpoint.generation) && !checkpoint.ack.isCompleted &&
              owner.exists(_.pending.containsKey(checkpoint.ack)) =>
            if (state.invalid.nonEmpty || checkpoint.ordinal != e.completedCursors.size ||
                plan.lift(checkpoint.ordinal) != Some(checkpoint.cursor) ||
                state.plan.size != plan.size || state.plan.lift(checkpoint.ordinal) != Some(checkpoint.cursor)) {
              checkpoint.ack.tryFailure(new IllegalStateException("checkpoint identity/frontier mismatch"))
              closeOwner()
              Effect.stop()
            } else {
              val encoded = try Right(stateCodec.encode(checkpoint.snapshot)) catch { case NonFatal(ex) => Left(ex) }
              encoded match {
                case Left(ex) =>
                  checkpoint.ack.tryFailure(ex)
                  ctx.log.error("Monarch checkpoint encode failed for {}", persistenceId.id, ex)
                  closeOwner()
                  Effect.stop()
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
                      // A notification failure cannot suppress the durable acknowledgement.
                      observer match {
                        case durable: DurableRunObserver[S] => checkpoint.resolution match {
                          case Some(error) => observe(durable.onResolutionCommitted(checkpoint.cursor, error, checkpoint.snapshot))
                          case None => observe(durable.onCheckpointCommitted(checkpoint.cursor, checkpoint.metadata, checkpoint.snapshot))
                        }
                        case _ => ()
                      }
                      checkpoint.ack.trySuccess(())
                    } else checkpoint.ack.tryFailure(StaleRun)
                  }
              }
            }
          case _ => checkpoint.ack.tryFailure(StaleRun); Effect.none
        }

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
            case None if e.completedCursors == plan && state.invalid.isEmpty =>
              Effect.persist(RunCompleted(e.runId)).thenRun { (_: DurableState) =>
                closeOwner()
                observe(observer.onCompleted(e.runId))
              }
            case Some((cursor, reason)) => Effect.persist(RunFailedEvt(cursor, reason)).thenRun { (_: DurableState) =>
              closeOwner()
              observe(observer.onFailed(e.runId, cursor, reason))
            }
            case _ => closeOwner(); Effect.stop()
          }
          case _ => Effect.none
        }

        case ResumeRun(generation) if current(generation) => state.run match {
          case e: Executing if state.plan != plan =>
            Effect.persist(RunPlanExtended(plan)).thenRun { (_: DurableState) => self ! ResumeRun[S](generation) }
          case e: Executing =>
            val decoded = try Right(stateCodec.decode(e.lastSnapshot)) catch { case NonFatal(ex) => Left(ex) }
            decoded match {
              case Right(snapshot) => runEngine(snapshot, e.completedCursors.size, owner.get); Effect.none
              case Left(ex) =>
                ctx.log.error("Monarch {} recovery decode failed", persistenceId.id, ex)
                closeOwner()
                Effect.stop()
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
      case (state, RecoveryCompleted) => state.run match {
        case e: Executing => recoveryProblem(state, plan) match {
          case Some(problem) =>
            ctx.log.error("Monarch {} recovery rejected; manual recovery required: {}", persistenceId.id, problem)
            closeOwner()
            // Remain inert without rewriting an ambiguous journal.
          case None =>
            val next = acquireOwner()
            observe(observer.onRecovery(e.runId, e.completedCursors.size))
            self ! ResumeRun[S](next.lease.generation)
        }
        case _ => ()
      }
      case (_, failure: SnapshotFailed) => ctx.log.warn("Monarch {} snapshot save failed", persistenceId.id, failure.failure)
    }

    // Lifecycle interception surrounds the ESB, including replay/persist phases.
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
