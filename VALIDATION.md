# Historical 0.2.1 candidate validation

Environment: local macOS / JDK 11.0.11, Scala 2.13.14, sbt 1.4.9, Akka 2.6.20. At the time of this historical validation, both independent repositories were on `monarch-0.2.1-hardening` and their changes were uncommitted. No tag, remote push, Maven Central publication, application regeneration, or deletion of 0.2 occurred.

All counts, measurements, paths and commands below refer only to that historical `0.2.1` candidate, not the current branch or frozen R2/core pair. Current R1/R2 evidence is recorded separately in [docs/ACCEPTANCE.zh.md](docs/ACCEPTANCE.zh.md). This documentation checkpoint reruns no tests, builds, benchmarks or publication commands; it does not replace any frozen artifact.

## Gates

- Core: 34 tests passed, including the original 16. Four initial defect regressions were executed against the old implementation and failed before the fixes; they now pass.
- Persistence: 33 regular tests plus one explicitly opt-in capacity test. The capacity test is cancelled in ordinary suite execution and was separately executed for all three samples below.
- MiMa against published 0.2 passes for both libraries. Existing constructor/factory/named-parameter, legacy hooks, builder methods, protocol case-class shapes and public behavior signature remain available.
- `Test / fork` resolves to false in both builds. Forking, test parallelism and actor/Future dispatchers are different settings.
- Persistence resolves the actual core candidate jar from an isolated Maven directory. This is not a sibling source dependency or a shared ~/.ivy2/local override. Final local integration path: `monarch-core/target/candidate-validated/`.
- Generated protobuf types, converter and serializer compile; new durable writes select MonarchSerializer. Empty/unknown variants and unsupported schema/manifests fail instead of dropping an event.
- Old Jackson serializer IDs/manifests remain readable. `legacy-0.2-events.txt` was produced by launching LegacyEventFixtureMain with the real published monarch-persistence_2.13-0.2.jar ahead of candidate classes; its recorded source location was checked. Fixed bytes cover all four old event variants. Mixed old Jackson / new Protobuf journal replay and append-only plan extension are tested.
- Controlled journal tests gate start, clean checkpoint and terminal acknowledgements; the next stage does not start before commit. Resolved state and the continuous durable prefix are atomic. Legacy holes, duplicates and plan rewrites do not start execution.
- Stop during initial persistence, checkpoint persistence and journal replay, same-ref supervised restart with queued old callbacks, late results, journal/codec errors and checkpoint timeout are covered. Checkpoint infrastructure errors do not invoke business regrouping policy or fabricate a failure terminal.
- Observer NonFatal failures are logged and isolated. Execution completion, checkpoint commit and terminal commit notifications are separate, not exactly-once business hooks.
- Snapshot tests confirm an actual serialized snapshot at sequence 2 is loaded and only sequence 3 is replayed; snapshot-save failure recovers from the full journal. No journal deletion is enabled.
- Two real child JDK 11 processes are forcibly killed and restarted using a test-only file journal that forces serialized frames to disk. A clean/resolved durable prefix resumes with matching state; uncommitted tail executes again. A forced tail whose ack was lost is recovered and skipped.
- Generic StartRun[S], representative nontrivial S and a usable reply ActorRef are serializer-roundtripped with an explicit host binding; reply variants also roundtrip. This is not a live multi-node remoting test.
- Core hook-reference inspection checks both ordinary and checkpoint modes have no builder/$outer reference. No forced-GC assertion is used to imply a heap profile.

## Measured delayed-journal capacity

The test-only journal serializes Protobuf into memory and uses an asynchronous 1ms scheduled delay. Akka's scheduler tick/runtime overhead produced roughly 30ms effective latency, not a measured production database latency. Fixed-size state; snapshot disabled. Pending count is journal checkpoint writes, not total application memory. Heap figures include sbt/test framework and retained test journal data, without forced GC.

| Checkpoints | S bytes | Max pending | Elapsed | Checkpoints/s | p50 / p95 / p99 journal ms | Event payload bytes | Recovery ms |
|---:|---:|---:|---:|---:|---|---:|---:|
| 10,000 | 1,024 | 1 | 300.29s | 33.30 | 29.713 / 31.644 / 32.261 | 10,605,609 | 113.97 |
| 1,000 | 16,384 | 1 | 30.24s | 33.07 | 29.422 / 31.430 / 32.203 | 16,418,606 | 45.59 |
| 100 | 1,048,576 | 1 | 3.33s | 30.02 | 28.230 / 30.695 / 68.038 | 104,860,705 | 100.84 |

Encode totals: 18.197ms / 6.032ms / 52.578ms respectively. Heap before/after: 255,455,872/417,985,152; 146,818,768/245,384,912; 255,620,392/378,954,640 bytes. GC collection-time delta: 0/0/52ms. These readings do not measure live-set size or prove bounded total heap.

Each sample verified all checkpoint counts and continuous recovery state, no missing terminal, and max pending=1. Larger payload samples are capped at a 256MiB payload budget. Do not interpret these as production throughput promises. No 100,000-checkpoint real journal stress test was run.

## Core microbenchmark

Independent JDK 11 processes with -Xms64m -Xmx512m, fixed two-thread execution context, compact Long state and immediately successful stage body. The exact same compiled test entrypoint runs against published 0.2 or candidate classes; no new API is needed. Single sample per size, no steady-state warm-up/JMH/confidence interval.

| Stages | 0.2 load ms | Candidate load ms | 0.2 run ms | Candidate run ms |
|---:|---:|---:|---:|---:|
| 10,000 | 21.697 | 23.520 | 124.024 | 123.964 |
| 100,000 | 70.646 | 37.689 | 379.306 | 453.670 |
| 1,000,000 | 270.927 | 143.610 | 855.839 | 1,133.426 |

The candidate reduces plan-loading work at larger sizes, but the million-empty-stage run was about 32% slower in this sample because of added safety/control-flow checks. An earlier implementation charged empty checkpoint Futures to all builders; that unnecessary path was removed before these final numbers. No safety check was removed to make the benchmark win. This remains a flow-control library, not a record-per-stage streaming system.

Million-stage heap before/after: baseline 11,534,336/171,944,544; candidate 12,582,912/82,891,696 bytes; GC time 138/89ms. Transient allocation and GC timing make these non-comparable as retained heap measurements. High key turnover tests confirm strict leases release owners; legacy RunRegistry historical key retention intentionally remains unchanged.

## Reproduce

Use JDK 11; commands run inside the relevant repository. See README for creating an isolated core candidate artifact. From persistence:

```bash
sbt -Dmonarch.core.repository=file:///absolute/path/to/monarch-core/target/candidate-validated/ \
  test mimaReportBinaryIssues
sbt -Dmonarch.core.repository=file:///absolute/path/to/monarch-core/target/candidate-validated/ \
  -Dmonarch.capacity=true 'testOnly net.imadz.monarch.persistence.PersistenceCapacitySpec'
# additional flags: -Dmonarch.capacity.stages=1000 -Dmonarch.capacity.bytes=16384
# or: -Dmonarch.capacity.stages=100 -Dmonarch.capacity.bytes=1048576
```

Core: `sbt test mimaReportBinaryIssues`; after Test / compile, launch CoreCapacityMain with test classes, the desired engine jar/classes and scala-library on an isolated JVM classpath. Capture baseline and candidate using the same flags. Do not run tests from the workspace root.

## Explicit limits

No production MongoDB/JDBC journal benchmark, multi-node cluster ownership test, exactly-once sink, full semiconductor/banking workflow, JMH/JFR/retained-heap profile or unlimited live queue was implemented/verified. The plan and cursor history remain O(N); window=1 bounds only checkpoint accumulation. Applications own chunking, resource admission, idempotent external effects, durable artifact availability and regrouping/rescheduling business logic. New-format writes cannot be blindly downgraded to 0.2. Published 0.2 need not be removed.
