# monarch-persistence

English | [中文](README-zh.md)

## Current R1/R2 entry points

The current R2 candidate is `0.3.0-dev.r2-20261106-dc168f96fa62`, paired with the fixed `monarch-core` `0.2.1` artifact. This frozen pair is not a claim of a formal `0.3.0` release.

- R1: `PersistentMonarch.managedBehavior` provides recoverable external-operation wiring.
- R2: `WorkflowHost` / `WorkflowProgram` and `PersistentInbox` provide finite jobs, bounded resource coordination and durable child-task handoff, reusing the existing stage engine rather than replacing it.

See the [R1 guide](docs/RECOVERABLE_OPERATIONS.zh.md), [R2 guide](docs/DURABLE_WORKFLOWS.zh.md), [R1/R2 acceptance record](docs/ACCEPTANCE.zh.md) and [examples guide](examples/README.md). These detailed guides are currently in Chinese.

The walkthrough below covers the **legacy `PersistentMonarch.behavior` / `behaviorWithSettings` checkpoint wrapper**, not the R2 workflow API. Its historical `0.2.1` counts and publication examples are not new R1/R2 test results or instructions to replace the frozen pair. Links to the sibling core tutorial are workspace references requiring that separate checkout, not a clean-checkout guarantee for this repository.

## Legacy checkpoint-wrapper walkthrough

Akka Persistence support for the framework-free [monarch-core](../monarch-core/README.md). Core passes a progress card through your stages; this wrapper records the card and accepted progress in a journal, so a recovered actor can continue. One entity owns one run.

**Historical 0.2.1 baseline—not the current R2 summary or a remote-release claim.** It added acknowledged checkpoints, classified-resolution persistence, strict JVM ownership, Protobuf wire formats and optional snapshots. Earlier validation records 34 core tests, 33 regular persistence tests and MiMa compatibility with published 0.2 for both libraries; this documentation update did not rerun them. Coverage and measurements are in [VALIDATION.md](VALIDATION.md). These are library checks, not full banking/Fab business acceptance or production performance guarantees.

## Quick start

If Stage, State and interpreter are new to you, start with the [complete core console example](../monarch-core/README.md#first-one-successful-console-run). We use the same recharge batch here: prepare its file, submit it, then summarize. The wrapper supplies durable progress, not the recharge implementation or a child-batch scheduler.

The following is **application-wiring pseudocode**, not a standalone program. `RechargeConsole` refers to the application object from the core guide; `progressToBytes` and `progressFromBytes` are the application's version-compatible codec functions. Create the behavior on the actor thread, capture the execution context there, and spawn it as an actor (or register it with Cluster Sharding) before sending `StartRun`.

```scala
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import net.imadz.monarch.persistence._
import scala.concurrent.ExecutionContext
import RechargeConsole.{Progress, PrepareFile, SubmitBatch, Summarize, runStage}

// parentBehavior must itself be started by the application's ActorSystem.
val parentBehavior = Behaviors.setup[Nothing] { context =>
  implicit val ec: ExecutionContext = context.executionContext
  val runId = "recharge-001"
  val behavior = PersistentMonarch.behavior[RechargeConsole.Stage, Progress](
    persistenceId = PersistenceId("recharge-run", runId),
    stages = Vector(PrepareFile, SubmitBatch, Summarize),
    interpreter = (stage, state) => runStage(stage, state, rejectBatch = false),
    stateCodec = StateCodec[Progress](progressToBytes, progressFromBytes),
    stageName = stage => stage.name
  )
  val runActor = context.spawn(behavior, "recharge-run")
  val replies = context.spawn(Behaviors.setup[PersistentMonarch.RunReply] { replyContext =>
    Behaviors.receiveMessage[PersistentMonarch.RunReply] { reply =>
      replyContext.log.info("Run start reply: {}", reply)
      Behaviors.same
    }
  }, "run-replies")
  runActor ! PersistentMonarch.StartRun(runId, Progress(runId, requestCount = 5L), replies)
  Behaviors.empty
}
```

Before starting the ActorSystem, configure and initialize an Akka Persistence **journal plugin** and its backing store. Adding this library does not install or configure a database. If snapshots are enabled, configure a snapshot store too; they are off by default. The example's `demo://` file reference remains simulated—real recovery needs an accessible durable artifact. Do not call ActorContext operations from the interpreter's Future callbacks; capture required ActorRefs on the actor thread.

`Accepted` replies only after `RunStarted` commits. It means “the run start is recorded,” not “the recharge batch succeeded.” A terminal entity rejects another `StartRun`; use a new `persistenceId` for a new run.

## Resolve a classified rejection, then continue the tail

Add `failurePolicy = Some(reviewRejectedSubmission)` to the `behavior` call after defining this policy in the enclosing application code. To see the rejection in the simulation, also use `rejectBatch = true` in the interpreter:

```scala
import net.imadz.monarch.{StageError, StageFailedException}
import scala.concurrent.Future

val reviewRejectedSubmission: (String, StageError, Progress) => Future[Progress] =
  (_, error, before) =>
    if (error.code.contains("BATCH_REJECTED"))
      Future.successful(before.copy(reviewRequired = true))
    else Future.failed(StageFailedException(error))
```

The policy receives the state **from before the failed stage**. A successful return records **StageResolved**, not clean StageDone. Once that checkpoint commits, `Summarize` receives the review state; submission is not automatically retried. Both clean and resolved checkpoints advance the continuous accepted prefix with its matching state.

Applications may classify failed items, split groups and return durable child-task references to an independent bounded scheduler. The legacy `PersistentMonarch.behavior` wrapper does not supply those business rules, create child tasks or dynamically insert stages into the current run. R2's finite jobs and durable child-task handoff are separate APIs described in the [R2 guide](docs/DURABLE_WORKFLOWS.zh.md), not capabilities of this legacy wrapper. Policies and external effects must tolerate re-execution because an unconfirmed stage or resolution can run again.

Checkpoint/codec/journal failures do **not** enter the business policy. A checkpoint timeout may have an unknown commit outcome; it is not grounds for automatic regrouping or compensation.

## Recovery and ownership

For the recharge example, imagine `prepare-file#0` and `submit-batch#1` have committed. Recovery restores the submission card, validates that these two cursors form the plan's leading prefix, and runs `summarize#2`. If submission was resolved for review, its `StageResolved` card is restored instead. If no submission checkpoint committed, submission may run again; if it committed but the acknowledgement was lost, replay finds it and skips it.

The wrapper rebuilds the core plan in a new engine from the supplied stage list, allocating positions from zero. Keep that order, naming and business meaning stable. Core's separate mutable plan methods do not update this actor's recorded plan or inject work into its current run. Append-only extension below is a **recovery configuration change** for an executing entity, not a live scheduling API.

The main boundaries are:

- RunStarted is committed before replying Accepted or starting any stage. Accepted acknowledges the run start, not eventual business success.
- At most one checkpoint is unacknowledged per run. The next stage waits for its persist acknowledgement; RunCompleted also waits for the final stage checkpoint.
- An uncommitted stage may execute again after recovery. A committed checkpoint whose acknowledgement was lost is found during replay and skipped.
- Strict RunLease ownership is acquired on start/recovery and revoked on stop, restart, terminal completion or checkpoint infrastructure failure. All internal results carry an incarnation identity and are checked on receipt, not only before sending.
- This ownership is per JVM. Cluster ownership and fencing/idempotency at external systems remain the application's responsibility. Already-issued I/O is not cancelled.
- Recovery validates the recorded plan and completed cursors as a continuous prefix. Tail additions are supported and recorded through RunPlanExtended before execution. Deleting/reordering the recorded plan, duplicate checkpoints and holey historical progress are rejected before starting the interpreter.
- Old resolved holes cannot safely be repaired from a later snapshot. The entity remains inert and logs a manual-recovery diagnostic rather than rewriting history. Plan names/positions cannot detect semantic changes to a same-named stage; stable business meaning remains a host requirement.
- One terminal entity rejects another StartRun; use a new persistenceId for a new run.

## Protobuf and compatibility

`src/main/protobuf/monarch.proto` defines events, run state and the complete snapshot envelope. ScalaPB generates managed sources; MonarchProtoConverters maps the Scala API to those types. MonarchSerializer currently writes E2 for VersionedRunStarted/RunAwaiting, E1 for other events, S2 for DurableState and R1 for RunState; reads also accept old S1 snapshots. Its serializer identifier remains 77102121. Do not reuse field numbers or alternatives when evolving the protocol. Unsupported schema versions/empty variants fail explicitly.

The library reference.conf binds new event/state/snapshot writes to this Protobuf serializer. StateCodec[S] still owns **business state bytes** and its version compatibility; it is not an outer event or remote command serializer.

0.2 Jackson-CBOR/JSON records retain their stored serializer identifier and class manifest. Jackson remains available for reading the four published event types; new writes use Protobuf. Normal old journal recovery and mixed Jackson/Protobuf replay are tested. A host with more specific event bindings must align them with the intended writer format and preserve the old reader. Do not delete/re-publish 0.2, run old and new writers concurrently for one persistenceId, or downgrade after writing new events/snapshots without a migration plan.

Internal checkpoint commands carry a local Promise and are never journaled or remotely serialized. StartRun[S] crossing node boundaries needs an explicit **host command serializer/binding** covering S and replyTo. RemoteCommandCompatibilitySpec demonstrates a representative state and a usable ActorRef roundtrip; it is not a multi-node network test. Generic arbitrary S cannot be automatically encoded by the library.

## Settings and observability

The original behavior signature is preserved. Configure the additive API when needed. This is host-wiring pseudocode: `persistenceId`, `stages`, `interpreter` and `codec` come from the enclosing application, which must also provide an implicit `ExecutionContext`.

```scala
import net.imadz.monarch.persistence.{PersistentMonarch, PersistentMonarchSettings}
import scala.concurrent.duration._
val configured = PersistentMonarch.behaviorWithSettings(
  persistenceId, stages, interpreter, codec,
  PersistentMonarchSettings(
    checkpointTimeout = 30.seconds,
    snapshotEvery = Some(1000),
    keepSnapshots = 2
  )
)
```

Default checkpoint timeout: 30 seconds, tuned by the host to the journal SLA. Expiry independently fails the outstanding acknowledgement and revokes execution, even while the actor waits for journal I/O. The actor stops when able to process the failure; the library neither retries the uncertain write nor records a fabricated business failure. Recovery must determine whether it committed. A permanently hanging journal still requires host operational handling.

Snapshots default to disabled. Enabled snapshots include the plan, run state, continuous progress, codec bytes and validation version. Snapshot + journal-tail replay is tested, as is full journal recovery after snapshot save failure. Snapshots reduce replay, not event write amplification. Old snapshots may be removed by snapshot retention; **journal event deletion is not enabled**.

RunObserver is a non-blocking notification interface. NonFatal observer exceptions are logged and isolated from execution control. onStageComplete means execution completion, not durable acceptance. DurableRunObserver adds onCheckpointCommitted and onResolutionCommitted; onCompleted occurs after the terminal event commits. Neither observer is an exactly-once business-effect hook.

## Large datasets

Use stages for meaningful process/transaction/recovery boundaries. Run bounded streams/chunks within a stage, preserving compact offsets/counts and durable artifact references in S. Do not turn every record into a stage or accumulate the full history in every checkpoint. The plan and cursor history remain O(N); window=1 bounds checkpoint accumulation, not total application memory or concurrent runs.

A compact state may describe a source partition/offset, sink idempotency key, and a durable output artifact. The application's stage interpreter implements bounded reads and sink writes; its policy supplies regrouping/rescheduling references. The library does not infer an exactly-once source/sink transaction from this state.

## Build, tests and local candidates

JDK 11, Scala 2.13.14, sbt 1.4.9. Run commands in this repository, not the workspace root. Earlier validation records `Test / fork = false`; you can inspect it with `sbt 'show Test / fork'`. Forking, suite parallelism and actor/Future dispatchers are separate settings. The commands below are reference instructions, not commands run for this documentation update.

```bash
sbt test mimaReportBinaryIssues
```

The following is the historical `0.2.1` isolated-candidate setup, not instructions to rebuild or replace the current frozen R2/core pair. The historical validation used an isolated Maven repository for its unpublished core `0.2.1` candidate, rather than overwriting shared ~/.ivy2/local. Historical generation example from **monarch-core**:

```bash
sbt 'set version := "0.2.1"' \
  'set publishTo := Some(Resolver.file("candidate", file("target/isolated-maven"))(Resolver.mavenStylePatterns))' publish
```

Then from **monarch-persistence**, supply that absolute file URL:

```bash
sbt -Dmonarch.core.repository=file:///absolute/path/to/monarch-core/target/isolated-maven/ \
  test mimaReportBinaryIssues
```

This publishes only to that local folder, not Maven Central. Inspect dependencyClasspath to confirm the actual candidate jar. The property is opt-in; no sibling source dependency is silently substituted.

The historical `0.2.1` validation suite included serialization-enabled persistence, controlled journal gates, owner stop/restart, legacy recovery, snapshots and two externally killed JDK 11 process tests using a test-only durable file journal. These are library-contract fixtures, not production database or full business acceptance tests.

Capacity runs are opt-in and payload-budgeted:

```bash
sbt -Dmonarch.core.repository=file:///absolute/candidate/repository/ \
  -Dmonarch.capacity=true 'testOnly net.imadz.monarch.persistence.PersistenceCapacitySpec'
# Optional sample sizes:
# -Dmonarch.capacity.stages=1000 -Dmonarch.capacity.bytes=16384
# -Dmonarch.capacity.stages=100 -Dmonarch.capacity.bytes=1048576
```

See [VALIDATION.md](VALIDATION.md) for actual coverage and measured limits. Capacity is intentionally cancelled in ordinary test runs; it is separately executed, not counted as regular suite coverage. The recorded samples cover 10,000 × 1 KiB, 1,000 × 16 KiB and 100 × 1 MiB state checkpoints. The [historical core single-sample comparison](VALIDATION.md#core-microbenchmark) includes a roughly 32% slower million-immediate-stage run for the candidate, alongside cheaper large-plan loading. None is a steady-state or production throughput claim.

## Release boundary

Versions are tag-derived. Tests and MiMa against 0.2 gate the existing tag-triggered publication workflow. The core-0.2.1-then-persistence-0.2.1 publication order describes only the historical 0.2.1 baseline, not replacement of the current frozen R2/core pair. Never overwrite the frozen pair or republish its versions. This documentation commit authorizes no tags, push, artifact publication, application upgrades or journal migration, and updates no other runtime library; those actions still require separate authorization.

Keep version history in a **CHANGELOG**, a short entry link in README, and the user-facing summary/upgrade notes in **GitHub Releases**. [VALIDATION.md](VALIDATION.md) supplies evidence, not release notes. Creating a CHANGELOG or GitHub Release is a separate task and is not done here.
