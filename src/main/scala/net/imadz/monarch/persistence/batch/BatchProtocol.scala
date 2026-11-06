package net.imadz.monarch.persistence.batch

import akka.actor.typed.ActorRef
import akka.util.ByteString
import net.imadz.monarch.persistence.{ProtocolDigest, ProtocolLimits, RunSubmission}
import net.imadz.monarch.persistence.resource.ResourceProtocol
import net.imadz.monarch.persistence.workflow._

final case class BatchSettings(
    maxAcceptedItems: Int = 1000,
    maxActiveItems: Int = 100,
    maxActiveBytes: Int = 1024 * 1024,
    maxItemBytes: Int = 4096,
    maxResultBytes: Int = 4096,
    maxConcurrentBatches: Int = 4,
    maxTransitions: Int = 16,
    maxBatchCalls: Int = 300,
    maxReconciliations: Int = 3,
    pollIntervalMillis: Long = 250,
    callTimeoutMillis: Long = 5000,
    limits: ProtocolLimits = ProtocolLimits()
) {
  require(maxAcceptedItems > 0 && maxAcceptedItems <= 1000 && maxActiveItems > 0 && maxActiveItems <= maxAcceptedItems,
    "job/item capacity is invalid (R2 finite jobs have at most 1000 accepted items)")
  require(maxItemBytes > 0 && maxItemBytes <= 4096 && maxResultBytes > 0 && maxResultBytes <= 4096,
    "item/result must be at most 4096 bytes")
  require(maxActiveBytes > 0 && maxActiveBytes <= limits.maxEnvelopeBytes && maxConcurrentBatches > 0 &&
    maxConcurrentBatches <= 32 && maxTransitions > 0 && maxTransitions <= 32 && maxBatchCalls > 0 &&
    maxBatchCalls <= 100000 && maxReconciliations >= 0 && maxReconciliations <= 100,
    "invalid bounded job settings")
  require(pollIntervalMillis > 0 && pollIntervalMillis <= 86400000L && callTimeoutMillis > 0 &&
    callTimeoutMillis <= 86400000L, "invalid coordinator timer settings")
}

final case class BatchDefinition(id: String, version: String, areas: Vector[AreaDefinition],
    settings: BatchSettings = BatchSettings()) {
  require(id.nonEmpty && version.nonEmpty && areas.nonEmpty && areas.size <= 32 &&
    areas.map(_.id).distinct.size == areas.size, "workflow must have unique bounded areas")
  require(areas.forall(a => a.batch.maxItems <= settings.maxActiveItems &&
    a.batch.maxItems.toLong * settings.maxItemBytes <= settings.maxActiveBytes),
    "capacity must accommodate every area's legal batch")
  val byId: Map[String, AreaDefinition] = areas.map(a => a.id -> a).toMap
  def fingerprint: String = BatchCodec.definitionDigest(this)
}

/** Host-only pure mapping: no effects in prepare/classify. Side effects execute in the child Monarch run. */
trait BatchExecutionPort {
  def prepare(manifest: BatchManifest, grant: ResourceProtocol.Grant): Either[String, RunSubmission]
  def classify(manifest: BatchManifest, result: ByteString): Either[String, BatchCompletion]
  /** Explicit allowlist/contract for a query-only verification step; never a blind business retry. */
  def verifyRoute(item: Item, area: String): Either[String, Unit] = Left("no verification route registered")
}

object BatchProtocol {
  sealed trait Command
  sealed trait Wire
  sealed trait Reply extends Wire
  final case class Offer(sequence: Long, itemId: String, payload: ByteString, area: String,
      compatibilityKey: String, replyTo: ActorRef[Reply]) extends Command with Wire {
    def fingerprint: String = ProtocolDigest.fields(Vector(ByteString(sequence.toString), ByteString(itemId), payload,
      ByteString(area), ByteString(compatibilityKey)))
  }
  final case class CloseInput(replyTo: ActorRef[Reply]) extends Command with Wire
  final case class Get(replyTo: ActorRef[Reply], afterSequence: Long = -1L, limit: Int = 100) extends Command with Wire
  final case class Reconcile(batchId: String, replyTo: ActorRef[Reply]) extends Command with Wire
  /** Only a held item may enter a registered verification area. This is not force-success or blind retry. */
  final case class VerifyItem(sequence: Long, verificationArea: String, compatibilityKey: String,
      replyTo: ActorRef[Reply]) extends Command with Wire
  final case class Accepted(sequence: Long, itemId: String, duplicate: Boolean) extends Reply
  final case class Backpressured(reason: String) extends Reply
  final case class Rejected(reason: String) extends Reply
  final case class Current(view: View) extends Reply
  final case class ItemView(sequence: Long, itemId: String, area: String, phase: String, executions: Map[String, Int],
      reason: Option[String], result: ByteString)
  final case class BatchView(batchId: String, area: String, phase: String, itemIds: Vector[String],
      reservationId: Option[String], deliveryId: Option[String], reason: Option[String])
  final case class View(jobId: String, phase: String, inputClosed: Boolean, accepted: Long,
      succeeded: Int, failed: Int, held: Int, active: Int, activeBytes: Long,
      batches: Vector[BatchView], items: Vector[ItemView], truncated: Boolean, reason: Option[String])

  private[persistence] final case class Drive(generation: Long, token: Long) extends Command
  private[persistence] sealed trait CallResult
  private[persistence] final case class ResourceResult(reply: ResourceProtocol.Reply) extends CallResult
  private[persistence] final case class DeliveryResult(reply: net.imadz.monarch.persistence.delivery.DeliveryProtocol.Reply) extends CallResult
  private[persistence] final case class Called(generation: Long, batchId: String, callId: Long,
      result: scala.util.Try[CallResult]) extends Command
  private[persistence] final case class CallTimeout(generation: Long, batchId: String, callId: Long) extends Command
}
