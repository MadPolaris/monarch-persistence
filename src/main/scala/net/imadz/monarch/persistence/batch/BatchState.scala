package net.imadz.monarch.persistence.batch

import net.imadz.monarch.persistence.delivery.DeliveryProtocol
import net.imadz.monarch.persistence.resource.ResourceProtocol
import net.imadz.monarch.persistence.workflow._

private[persistence] final case class ItemRecord(item: Item, inputDigest: String, phase: String,
    acceptedAt: Long, dueAt: Long, batchId: Option[String] = None, result: Vector[Byte] = Vector.empty,
    reason: Option[String] = None)
private[persistence] final case class ActiveBatch(manifest: BatchManifest, phase: String = "Reserving",
    grant: Option[ResourceProtocol.Grant] = None, delivery: Option[DeliveryProtocol.Request] = None,
    completion: Option[BatchCompletion] = None, carry: Option[ResourceClaim] = None,
    releasedCarried: Int = 0, calls: Int = 0, nextDue: Long = 0L,
    reason: Option[String] = None, resumePhase: Option[String] = None, reconciliations: Int = 0)
private[persistence] final case class BatchState(jobId: Option[String] = None, definitionDigest: Option[String] = None,
    nextSequence: Long = 0L, nextBatch: Long = 0L, closed: Boolean = false,
    records: Map[Long, ItemRecord] = Map.empty, batches: Map[String, ActiveBatch] = Map.empty,
    invalid: Option[String] = None)
private[persistence] sealed trait BatchEvent
private[persistence] object BatchEvent {
  final case class Initialized(jobId: String, definitionDigest: String) extends BatchEvent
  final case class InputAccepted(record: ItemRecord) extends BatchEvent
  final case class Sealed(batch: ActiveBatch) extends BatchEvent
  final case class Advanced(batch: ActiveBatch) extends BatchEvent
  final case class Settled(batchId: String, records: Vector[ItemRecord]) extends BatchEvent
  final case class Verified(record: ItemRecord) extends BatchEvent
  case object InputClosed extends BatchEvent
}
private[persistence] object BatchState {
  def evolve(state: BatchState, event: BatchEvent): BatchState = {
    import BatchEvent._
    def invalid(reason: String): BatchState = state.copy(invalid = state.invalid.orElse(Some(reason)))
    if (state.invalid.nonEmpty) state else event match {
      case Initialized(id, digest) if state.jobId.isEmpty => state.copy(jobId = Some(id), definitionDigest = Some(digest))
      case InputAccepted(record) if state.jobId.nonEmpty && !state.closed && record.item.sequence == state.nextSequence &&
          record.phase == "Pending" && record.batchId.isEmpty && record.result.isEmpty && record.reason.isEmpty &&
          !state.records.valuesIterator.exists(_.item.itemId == record.item.itemId) =>
        state.copy(nextSequence = state.nextSequence + 1, records = state.records.updated(record.item.sequence, record))
      case Sealed(batch) if batch == ActiveBatch(batch.manifest) &&
          state.jobId.contains(batch.manifest.jobId) && !state.batches.contains(batch.manifest.batchId) &&
          batch.manifest.batchId == s"${batch.manifest.jobId}/batch-${state.nextBatch}" &&
          batch.manifest.items.nonEmpty && batch.manifest.items.map(_.sequence).distinct.size == batch.manifest.items.size &&
          batch.manifest.items.forall(item => state.records.get(item.sequence).exists(r => r.phase == "Pending" && r.item == item)) =>
        val updated = batch.manifest.items.foldLeft(state.records) { (records, item) =>
          records.updated(item.sequence, records(item.sequence).copy(phase = "Running", batchId = Some(batch.manifest.batchId)))
        }
        state.copy(nextBatch = state.nextBatch + 1, records = updated, batches = state.batches.updated(batch.manifest.batchId, batch))
      case Advanced(batch) if state.batches.get(batch.manifest.batchId).exists(BatchProgress.validAdvance(_, batch)) =>
        state.copy(batches = state.batches.updated(batch.manifest.batchId, batch))
      case Settled(id, records) if state.batches.get(id).exists(batch =>
          BatchProgress.validSettlement(batch, records, state.records)) =>
        state.copy(records = state.records ++ records.map(r => r.item.sequence -> r), batches = state.batches - id)
      case Verified(record) if state.records.get(record.item.sequence).exists(BatchProgress.validVerification(_, record)) =>
        state.copy(records = state.records.updated(record.item.sequence, record))
      case InputClosed if state.jobId.nonEmpty => state.copy(closed = true)
      case _ => invalid("invalid batch event/frontier")
    }
  }
}
