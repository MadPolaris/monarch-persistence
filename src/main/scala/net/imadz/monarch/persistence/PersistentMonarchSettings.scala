package net.imadz.monarch.persistence

import scala.concurrent.duration._

/** One checkpoint is in flight per run. Snapshots are opt-in; journal deletion
  * is never enabled by these settings. Choose the timeout for the journal SLA.
  */
final case class PersistentMonarchSettings(
    checkpointTimeout: FiniteDuration = 30.seconds,
    snapshotEvery: Option[Int] = None,
    keepSnapshots: Int = 2
) {
  require(checkpointTimeout.length > 0, "checkpointTimeout must be positive")
  require(snapshotEvery.forall(_ > 0), "snapshotEvery must be positive")
  require(keepSnapshots > 0, "keepSnapshots must be positive")
}
