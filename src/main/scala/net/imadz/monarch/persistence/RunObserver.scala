package net.imadz.monarch.persistence

/** Host observability for a persistent run. Journaling stays internal to the
  * behavior; the observer never influences control flow. All methods no-op
  * by default — override only what you need (metrics, audit log, websocket…).
  */
trait RunObserver[S] {
  def onStart(runId: String): Unit = ()
  def onStageComplete(cursor: String, metadata: Map[String, String], snapshot: S): Unit = ()
  def onStageFailed(cursor: String, detail: String): Unit = ()
  def onRecovery(runId: String, completedCount: Int): Unit = ()
  def onCompleted(runId: String): Unit = ()
  def onFailed(runId: String, cursor: String, reason: String): Unit = ()
}

/** Optional durable notifications. These are distinct from execution completion;
  * a resolved checkpoint advances recovery progress without being a clean success.
  */
trait DurableRunObserver[S] extends RunObserver[S] {
  def onCheckpointCommitted(cursor: String, metadata: Map[String, String], snapshot: S): Unit = ()
  def onResolutionCommitted(cursor: String, error: net.imadz.monarch.StageError, snapshot: S): Unit = ()
}

object RunObserver {
  def nop[S]: RunObserver[S] = new RunObserver[S] {}
}
