package net.imadz.monarch.persistence

import akka.util.ByteString
import net.imadz.monarch.persistence.batch._
import net.imadz.monarch.persistence.delivery.DeliveryProtocol
import net.imadz.monarch.persistence.resource.ResourceProtocol
import net.imadz.monarch.persistence.workflow._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BatchReplayGuardSpec extends AnyWordSpec with Matchers {
  private val area = AreaDefinition("a", "1", BatchPolicy.single, Map("machine" -> 1), maxExecutions = 1)
  private val item = Item(0, "item", Vector[Byte](1), "key", "a")
  private val manifest = BatchManifest("job", "job/batch-0", "a", "1", Vector(item), 100L)
  private val grant = ResourceProtocol.Grant(manifest.reservationId, manifest.operationKey, "token", Map("machine" -> 1))
  private val record = ItemRecord(item, "a" * 64, "Running", 10L, 10L, Some(manifest.batchId))
  private val child = RunSubmission(manifest.runId, "definition", "1", "1", ByteString("input"))
  private val delivery = DeliveryProtocol.Request(manifest.deliveryId, "job", "a", child, ByteString("digest"))
  private val routed = BatchCompletion(Vector(ItemDecision("item", ItemDisposition.Route("b", "key"))), ResourceDisposition.Release(Vector[Byte](1)))
  private val waiting = ActiveBatch(manifest, "WaitingChild", Some(grant), Some(delivery))
  private def state(batch: ActiveBatch): BatchState = BatchState(Some("job"), Some("b" * 64), 1, 1, closed = true,
    records = Map(0L -> record), batches = Map(manifest.batchId -> batch))

  "Batch replay guards" should {
    "reject a direct jump from WaitingChild to Settling or a changed frozen child" in {
      val forged = waiting.copy(phase = "Settling", completion = Some(routed))
      BatchState.evolve(state(waiting), BatchEvent.Advanced(forged)).invalid should not be empty
      BatchState.evolve(state(waiting), BatchEvent.Advanced(waiting.copy(delivery = Some(delivery.copy(target = "changed"))))).invalid should not be empty
      val valid = waiting.copy(phase = "ReleasingCarried", completion = Some(routed))
      BatchState.evolve(state(waiting), BatchEvent.Advanced(valid)).invalid shouldBe None
    }

    "reject pre-completed admission, pre-progressed sealing and history-resetting verification" in {
      val initialized = BatchState(Some("job"), Some("b" * 64))
      BatchState.evolve(initialized, BatchEvent.InputAccepted(record.copy(phase = "Succeeded", batchId = None))).invalid should not be empty
      val pending = record.copy(phase = "Pending", batchId = None)
      val accepted = BatchState.evolve(initialized, BatchEvent.InputAccepted(pending))
      BatchState.evolve(accepted, BatchEvent.Sealed(waiting)).invalid should not be empty
      BatchState.evolve(accepted, BatchEvent.Sealed(ActiveBatch(manifest))).invalid shouldBe None
      val held = pending.copy(phase = "Held", reason = Some("quality evidence"),
        item = pending.item.copy(executions = Map("a" -> 1), lineage = Vector("previous")))
      val before = accepted.copy(records = Map(0L -> held))
      val verified = held.copy(phase = "Pending", reason = None, item = held.item.copy(area = "b"), acceptedAt = 30, dueAt = 30)
      BatchState.evolve(before, BatchEvent.Verified(verified)).invalid shouldBe None
      Vector(verified.copy(phase = "Succeeded"), verified.copy(item = verified.item.copy(executions = Map.empty)),
        verified.copy(item = verified.item.copy(lineage = Vector.empty))).foreach { bad =>
        BatchState.evolve(before, BatchEvent.Verified(bad)).invalid should not be empty
      }
    }

    "require a transfer receipt before settling a carried occupancy" in {
      val carry = routed.copy(resource = ResourceDisposition.Carry("claim", "foup", Map("machine" -> 1), Vector[Byte](1)))
      val releasing = waiting.copy(phase = "Releasing", completion = Some(carry))
      val missing = releasing.copy(phase = "Settling")
      BatchState.evolve(state(releasing), BatchEvent.Advanced(missing)).invalid should not be empty
      intercept[IllegalArgumentException](BatchCodec.activeToProto(missing))
      val recorded = missing.copy(carry = Some(ResourceClaim("claim", "foup", "token", Map("machine" -> 1))))
      BatchState.evolve(state(releasing), BatchEvent.Advanced(recorded)).invalid shouldBe None
    }

    "settle only the exact recorded disposition and preserve input identity and execution history" in {
      val settling = waiting.copy(phase = "Settling", completion = Some(routed))
      val original = state(settling)
      val correct = BatchProgress.settle(settling, original.records, 200L)
      BatchState.evolve(original, BatchEvent.Settled(manifest.batchId, correct)).invalid shouldBe None
      Vector(correct.head.copy(phase = "Succeeded"), correct.head.copy(inputDigest = "c" * 64),
        correct.head.copy(item = correct.head.item.copy(itemId = "other")),
        correct.head.copy(item = correct.head.item.copy(executions = Map.empty))).foreach { bad =>
        BatchState.evolve(original, BatchEvent.Settled(manifest.batchId, Vector(bad))).invalid should not be empty
      }
    }

    "account for the current execution when routing back into the same area" in {
      val completion = routed.copy(decisions = Vector(ItemDecision("item", ItemDisposition.Route("a", "different-key"))))
      BatchConservation.validate(manifest, completion, Map("a" -> area), 16) should not be empty
    }

    "leave room for the final lineage record rather than creating an unpersistable last attempt" in {
      val exhausted = manifest.copy(items = Vector(item.copy(lineage = Vector.fill(31)("prior"))))
      val retry = BatchCompletion(Vector(ItemDecision("item", ItemDisposition.Retry(0))), ResourceDisposition.Release(Vector.empty))
      BatchConservation.validate(exhausted, retry, Map("a" -> area.copy(maxExecutions = 33)), 32) should not be empty
      val success = retry.copy(decisions = Vector(ItemDecision("item", ItemDisposition.Completed(Vector.empty))))
      BatchConservation.validate(exhausted, success, Map("a" -> area.copy(maxExecutions = 33)), 32) shouldBe None
    }

    "reject split delays for items sharing carried physical occupancy" in {
      val pair = manifest.copy(items = Vector(item, item.copy(sequence = 1, itemId = "other")))
      val split = BatchCompletion(Vector(ItemDecision("item", ItemDisposition.Route("b", "key", 0)),
        ItemDecision("other", ItemDisposition.Route("b", "key", 1000))),
        ResourceDisposition.Carry("claim", "foup", Map("machine" -> 1), Vector.empty))
      val areas = Map("a" -> area, "b" -> area.copy(id = "b"))
      BatchConservation.validate(pair, split, areas, 16) should not be empty
      intercept[IllegalArgumentException](BatchCodec.completionToBytes(split))
      val retain = split.copy(resource = ResourceDisposition.Release(Vector.empty), retainCarried = true)
      BatchConservation.validate(pair, retain, areas, 16) should not be empty
      intercept[IllegalArgumentException](BatchCodec.completionToBytes(retain))
    }
  }
}
