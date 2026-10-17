package net.imadz.monarch.persistence

import net.imadz.monarch.{Monarch, RunRegistry, StageError, StaleRun}

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, Recovery}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** PersistentMonarch — crash-recovery for the Monarch stage-queue engine.
  *
  * Monarch itself is framework-free: it walks a queue of stages and reports progress
  * through hooks, but journals nothing. This behavior wraps one Monarch run in an
  * Akka Persistence `EventSourcedBehavior`:
  *
  *   - every completed stage is journaled as `StageDone(cursor, snapshot)` —
  *     the snapshot is your state encoded by [[StateCodec]], so recovery can
  *     rebuild the exact resume point;
  *   - on `RecoveryCompleted` the journal replays to (completed cursors, last
  *     snapshot) and the run resumes via `Monarch.resume`, skipping only the
  *     finished prefix (cursor-based, tolerant of appended stages);
  *   - every run registers a [[RunRegistry]] generation at start and at recovery:
  *     a stale pre-crash Future chain dies silently at its next stage boundary
  *     instead of racing the recovered run or journaling twice.
  *
  * == Recovery sketch ==
  *
  * Journal holds: `RunStarted`, `StageDone("a#0", snap1)`, `StageDone("b#1", snap2)`
  *   → crash before "c" completes.
  * On restart: state = `Executing(completed=["a#0","b#1"], lastSnapshot=snap2)`,
  * `resume(decode(snap2), {"a#0","b#1"})` re-runs from "c".
  *
  * == What this does NOT do ==
  *
  *   - Stage idempotency: a crash mid-stage re-runs that stage (at-least-once).
  *     Stages must tolerate re-execution; Monarch only guarantees a finished
  *     stage is never re-run.
  *   - Cross-node generation fencing beyond one JVM: [[RunRegistry]] is per-JVM.
  *   - Akka snapshots: the journal alone is the source of truth (add
  *     `snapshotWhen` yourself if runs get long).
  *
  * One entity per run: the entityId IS the runId and the journal is per-entity,
  * so runs never replay each other's events.
  */
object PersistentMonarch {

  // ============================================================
  // Protocol
  // ============================================================

  sealed trait Command[S]

  /** Start a fresh run. `initialState` is what the first stage receives. */
  final case class StartRun[S](runId: String, initialState: S, replyTo: ActorRef[RunReply]) extends Command[S]

  /** Engine hook → self: a stage completed; persist its snapshot. */
  private[persistence] final case class StageCompleted[S](
      cursor: String,
      metadata: Map[String, String],
      snapshot: S
  ) extends Command[S]

  /** Engine hook → self: the whole queue drained. */
  private[persistence] final case class RunSucceeded[S]() extends Command[S]

  /** Engine hook → self: the run failed (after the failure policy ran). */
  private[persistence] final case class RunFailed[S](cursor: String, reason: String) extends Command[S]

  // ============================================================
  // Events — all Jackson-friendly; the journal never sees your `S` directly.
  // ============================================================

  sealed trait Event

  /** The initial snapshot rides along so recovery is self-contained even when
    * the crash happened before the first stage completed. */
  final case class RunStarted(runId: String, stageCursors: Vector[String], initialSnapshot: Array[Byte]) extends Event

  final case class StageDone(
      cursor: String,
      timestamp: Long,
      metadata: Map[String, String],
      snapshot: Array[Byte]
  ) extends Event

  final case class RunCompleted(runId: String) extends Event

  final case class RunFailedEvt(cursor: String, reason: String) extends Event

  // ============================================================
  // State
  // ============================================================

  sealed trait RunState
  case object Idle extends RunState

  final case class Executing(
      runId: String,
      completedCursors: Vector[String],
      lastSnapshot: Array[Byte]
  ) extends RunState

  final case class Done(runId: String) extends RunState
  final case class Failed(runId: String, cursor: String, reason: String) extends RunState

  // ============================================================
  // Replies
  // ============================================================

  sealed trait RunReply
  final case class Accepted(runId: String) extends RunReply
  final case class Rejected(runId: String, reason: String) extends RunReply

  // ============================================================
  // Behavior
  // ============================================================

  /**
   * @param persistenceId one entity per run; the journal is per-entity.
   * @param stages        the full stage queue. Must be deterministic across restarts:
   *                      recovery resumes by cursor against this queue.
   * @param interpreter   how to execute a single stage.
   * @param stateCodec    snapshot serialization for the journal.
   * @param stageName     stable stage names — they form the cursor vocabulary.
   * @param failurePolicy optional Monarch failure interceptor; without one a failed
   *                      stage fails the run.
   * @param observer      host observability; never influences control flow.
   */
  def behavior[Stage, S](
      persistenceId: PersistenceId,
      stages: Seq[Stage],
      interpreter: (Stage, S) => Future[S],
      stateCodec: StateCodec[S],
      stageName: Stage => String = (s: Stage) => String.valueOf(s),
      failurePolicy: Option[(String, StageError, S) => Future[S]] = None,
      observer: RunObserver[S] = RunObserver.nop[S]
  )(implicit ec: ExecutionContext): Behavior[Command[S]] = {

    Behaviors.setup { ctx =>
      val runKey = persistenceId.id

      /** Fresh generation: all previous runs of this key go stale. */
      def freshToken(): () => Boolean = {
        val gen = RunRegistry.register(runKey)
        () => RunRegistry.isFresh(runKey, gen)
      }

      /** Build a generation-guarded engine. Hooks re-check the token at send
        * time — a superseded run must not journal anything. */
      def engineFor(runToken: () => Boolean): Monarch[Stage, S] = {
        var b = Monarch
          .builder[Stage, S]
          .namingStages(stageName)
          .withExecution(interpreter)
          .withStages(stages)
          .withGenerationGuard(runToken)
          .onStageStart(_ => ())
          .onStageCompleteWithMeta { (cursor, state, metadata) =>
            if (runToken()) {
              observer.onStageComplete(cursor, metadata, state)
              ctx.self ! StageCompleted(cursor, metadata, state)
            }
          }
          .onStageFailed { (cursor, err) =>
            if (runToken()) {
              observer.onStageFailed(cursor, err.detail)
              ctx.log.warn("[PersistentMonarch:{}] stage failed at {}: {}", runKey, cursor, err.detail)
            }
          }
        failurePolicy.foreach(p => b = b.onFailure(p))
        b.build()
      }

      def runEngine(state: S, completed: Set[String], runToken: () => Boolean): Unit = {
        val engine = engineFor(runToken)
        engine.resume(state, completed).onComplete {
          case Success(_) =>
            if (runToken()) ctx.self ! RunSucceeded[S]()
          case Failure(StaleRun) =>
            () // superseded — die silently, journal nothing
          case Failure(NonFatal(e)) =>
            if (runToken()) ctx.self ! RunFailed[S]("pipeline", Option(e.getMessage).getOrElse(e.toString))
          case Failure(e) =>
            if (runToken()) ctx.self ! RunFailed[S]("pipeline", e.toString)
        }
      }

      val commandHandler: (RunState, Command[S]) => Effect[Event, RunState] = { (state, cmd) =>
        (state, cmd) match {

          case (Idle, StartRun(runId, initialState, replyTo)) =>
            val cursors = stages.zipWithIndex.map { case (st, i) => s"${stageName(st)}#$i" }.toVector
            Effect
              .persist(RunStarted(runId, cursors, stateCodec.encode(initialState)))
              .thenRun { _ =>
                val runToken = freshToken()
                observer.onStart(runId)
                runEngine(initialState, Set.empty, runToken)
                replyTo ! Accepted(runId)
              }

          case (e: Executing, StageCompleted(cursor, metadata, snapshot)) =>
            Effect.persist(StageDone(cursor, System.currentTimeMillis(), metadata, stateCodec.encode(snapshot)))

          case (e: Executing, RunSucceeded()) =>
            Effect.persist(RunCompleted(e.runId)).thenRun { _ =>
              observer.onCompleted(e.runId)
              ctx.log.info(
                "[PersistentMonarch:{}] run {} completed, {} stages",
                runKey,
                e.runId,
                e.completedCursors.size
              )
            }

          case (e: Executing, RunFailed(cursor, reason)) =>
            Effect.persist(RunFailedEvt(cursor, reason)).thenRun { _ =>
              observer.onFailed(e.runId, cursor, reason)
              ctx.log.error(
                "[PersistentMonarch:{}] run {} failed at {}: {}",
                runKey,
                e.runId,
                cursor,
                reason
              )
            }

          // Late engine callbacks after a terminal state — ignore.
          case (_: Done, _: StageCompleted[S])   => Effect.none
          case (_: Failed, _: StageCompleted[S]) => Effect.none

          case (s, StartRun(runId, _, replyTo)) =>
            replyTo ! Rejected(runId, s"run already in state ${s.getClass.getSimpleName}")
            Effect.none

          case _ => Effect.unhandled
        }
      }

      val eventHandler: (RunState, Event) => RunState = { (state, event) =>
        (state, event) match {
          case (Idle, RunStarted(runId, _, initialSnapshot)) =>
            Executing(runId, Vector.empty, initialSnapshot)

          case (e: Executing, StageDone(cursor, _, _, snapshot)) =>
            e.copy(completedCursors = e.completedCursors :+ cursor, lastSnapshot = snapshot)

          case (e: Executing, RunCompleted(_)) => Done(e.runId)

          case (e: Executing, RunFailedEvt(cursor, reason)) => Failed(e.runId, cursor, reason)

          // Idempotent replay guard: duplicate StageDone after a terminal event.
          case (_: Done, _: StageDone)   => state
          case (_: Failed, _: StageDone) => state

          case _ => state
        }
      }

      EventSourcedBehavior[Command[S], Event, RunState](
        persistenceId = persistenceId,
        emptyState = Idle,
        commandHandler = commandHandler,
        eventHandler = eventHandler
      ).withRecovery(Recovery.default)
        .receiveSignal {
          case (e: Executing, RecoveryCompleted) =>
            val runToken = freshToken()
            observer.onRecovery(e.runId, e.completedCursors.size)
            ctx.log.info(
              "[PersistentMonarch:{}] recovering run {}, {} stages already done",
              runKey,
              e.runId,
              e.completedCursors.size
            )
            try {
              val resumed = stateCodec.decode(e.lastSnapshot)
              runEngine(resumed, e.completedCursors.toSet, runToken)
            } catch {
              case NonFatal(ex) =>
                ctx.log.error(
                  "[PersistentMonarch:{}] snapshot decode failed for run {}",
                  runKey,
                  e.runId,
                  ex
                )
                ctx.self ! RunFailed[S]("recovery", s"snapshot decode failed: ${ex.getMessage}")
            }

          case _ => ()
        }
    }
  }

  // ============================================================
  // Cluster sharding wiring — one entity per run.
  // ============================================================

  /**
   * Register the entity type for cluster sharding. `behaviorFor` maps an
   * entityId (== runId) to its behavior; pass through your stages/interpreter/
   * codec there. Returns the type key for `entityRefFor` / messaging.
   */
  def initSharding[Stage, S](
      sharding: ClusterSharding,
      typeKeyName: String,
      behaviorFor: String => Behavior[Command[S]]
  ): EntityTypeKey[Command[S]] = {
    val key = EntityTypeKey[Command[S]](typeKeyName)
    sharding.init(Entity(key) { entityCtx => behaviorFor(entityCtx.entityId) })
    key
  }
}
