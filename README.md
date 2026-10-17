# monarch-persistence

Crash-recovery for the [monarch-core](https://github.com/MadPolaris/monarch-core) stage-queue engine, via Akka Persistence.

Monarch itself is framework-free: it walks a queue of stages and reports progress through hooks, but journals nothing. `PersistentMonarch` wraps one Monarch run in an Akka Persistence `EventSourcedBehavior`:

- every completed stage is journaled as `StageDone(cursor, snapshot)` — the snapshot is your state through a `StateCodec`, so recovery rebuilds the exact resume point;
- on `RecoveryCompleted` the journal replays to (completed cursors, last snapshot) and the run resumes via `Monarch.resume`, skipping only the finished prefix (cursor-based, tolerant of appended stages);
- every run registers a `RunRegistry` generation at start and at recovery: a stale pre-crash `Future` chain dies silently at its next stage boundary instead of racing the recovered run.

One entity per run: the entityId IS the runId, the journal is per-entity.

## Quick start

```scala
import net.imadz.monarch.persistence._
import akka.persistence.typed.PersistenceId

val behavior = PersistentMonarch.behavior[String, MyState](
  persistenceId = PersistenceId("my-run-type", runId),
  stages        = List("fetch", "validate", "settle"),
  interpreter   = (stage, state) => runOneStage(stage, state),  // Stage => State => Future[State]
  stateCodec    = StateCodec(MyState.toBytes, MyState.fromBytes),
  failurePolicy = Some((cursor, err, state) => compensate(cursor, err, state))
)

entityRef ! PersistentMonarch.StartRun(runId, initialState, replyTo)
```

With Cluster Sharding (one entity per run):

```scala
val typeKey = PersistentMonarch.initSharding(
  sharding, "my-run-type",
  entityId => PersistentMonarch.behavior(
    PersistenceId("my-run-type", entityId), stagesFor(entityId), interpreter, codec)
)
sharding.entityRefFor(typeKey, runId) ! PersistentMonarch.StartRun(runId, initial, replyTo)
```

## The recovery contract

| Event | When |
|---|---|
| `RunStarted(runId, cursors, initialSnapshot)` | run accepted — carries the initial snapshot so recovery works even if the crash hit before stage one finished |
| `StageDone(cursor, ts, metadata, snapshot)` | each stage completed |
| `RunCompleted(runId)` / `RunFailedEvt(cursor, reason)` | terminal |

Recovery = replay → `Executing(completedCursors, lastSnapshot)` → `resume(decode(lastSnapshot), completedCursors.toSet)`.

## What it does NOT do

- **Stage idempotency.** A crash mid-stage re-runs that stage (at-least-once). A *finished* stage is never re-run, but stages must tolerate re-execution.
- **Cross-JVM generation fencing.** `RunRegistry` is per-JVM; rolling deploys running the same run key concurrently need an external fence.
- **Akka snapshots.** The journal is the source of truth; add `snapshotWhen` yourself for very long runs.

## Serialization

Events are all Jackson-friendly (`String`/`Vector`/`Map`/`Array[Byte]`/`Long`) — your `S` never enters the journal directly, only its `StateCodec` bytes do. `StartRun` carries `initialState: S` in the command (not journaled); keep `S` Jackson-friendly if the command crosses node boundaries.

## Journal plugin

Any Akka Persistence journal works (the demo this was extracted from used MongoDB via `akka-persistence-mongo`). For tests, `akka-persistence-testkit`'s in-memory journal is on the test classpath — see `PersistentMonarchSpec`.

## Releasing

Push a tag like `v0.2` — GitHub Actions runs `sbt ci-release` to Maven Central as `net.imadz:monarch-persistence_2.13`. Version moves in lockstep with the family (`monarch-core`, `common-core`, `saga-core`, `auth-core`).
