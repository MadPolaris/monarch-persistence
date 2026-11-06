package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.serialization.{SerializationExtension, Serializers}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.batch._
import net.imadz.monarch.persistence.batch.BatchProtocol._
import net.imadz.monarch.persistence.batch.proto._
import net.imadz.monarch.persistence.delivery.DeliveryProtocol
import net.imadz.monarch.persistence.resource.ResourceProtocol
import net.imadz.monarch.persistence.workflow._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.immutable.ListMap
import scala.util.Success

/** Serializer-only tests: no BatchCoordinator, resource actor or child runtime is started. */
class BatchProtoSpec extends ScalaTestWithActorTestKit(
    ConfigFactory.parseString("akka.actor.allow-java-serialization = off")
      .withFallback(ConfigFactory.parseResources("batch-reference.conf")))
    with AnyWordSpecLike with Matchers {
  private val serialization = SerializationExtension(system.classicSystem)
  private val settings = BatchSettings()
  private val area = AreaDefinition("a", "policy-1", BatchPolicy.CountOrWait(10, 200, 2),
    ListMap("machine" -> 2, "operator" -> 1), parallelism = 2, maxExecutions = 5)
  private val definition = BatchDefinition("workflow", "v1", Vector(area, area.copy(id = "b")), settings)
  private val oldClaim = ResourceClaim("old-reservation", "old-operation", "old-token", Map("machine" -> 1))
  private val items = Vector.tabulate(5)(n => Item(n.toLong, s"item-$n", Vector[Byte](0, -1, 127), "compatible", "a",
    ListMap("a" -> 1, "b" -> 2), Vector("older-batch", "old-batch"), Vector(oldClaim)))
  private val manifest = BatchManifest("job", "job/batch-0", "a", "policy-1", items, 1234L)
  private val grant = ResourceProtocol.Grant(manifest.reservationId, manifest.operationKey, "grant-token",
    ListMap("operator" -> 1, "machine" -> 2))
  private val child = RunSubmission(manifest.runId, "child-definition", "child-v2", "codec-v3", ByteString(0, -1, 127))
  private val delivery = DeliveryProtocol.Request(manifest.deliveryId, manifest.jobId, "a", child, ByteString(1, -1, 0))
  private val completion = BatchCompletion(Vector(
    ItemDecision("item-2", ItemDisposition.Retry(100L)),
    ItemDecision("item-0", ItemDisposition.Completed(Vector[Byte](0, -1, 42))),
    ItemDecision("item-4", ItemDisposition.Hold("needs evidence")),
    ItemDecision("item-1", ItemDisposition.Failed("declined")),
    ItemDecision("item-3", ItemDisposition.Route("b", "next-key", 200L))),
    ResourceDisposition.Release(Vector[Byte](-1, 0, 127, 1)))
  private val carryCompletion = BatchCompletion(items.reverse.map(i => ItemDecision(i.itemId,
    ItemDisposition.Route("b", "together", 15L))),
    ResourceDisposition.Carry("next-reservation", "next-operation", Map("machine" -> 1), Vector[Byte](127, -1, 0)))
  private val carried = ResourceClaim("next-reservation", "next-operation", "next-token", Map("machine" -> 1))
  private val records = items.map(i => ItemRecord(i, ProtocolDigest.sha256(ByteString(i.itemId)), "Pending", 123L, 456L))
  private val reserving = ActiveBatch(manifest)
  private val preparing = reserving.copy(phase = "Preparing", grant = Some(grant), calls = 1, nextDue = 2000L)
  private val delivering = preparing.copy(phase = "Delivering", delivery = Some(delivery), calls = 2)
  private val waiting = delivering.copy(phase = "WaitingChild", calls = 3, nextDue = 3000L)
  private val reconciling = waiting.copy(phase = "ReconcilingChild", calls = 0, reconciliations = 1)
  private val releasingCarried = waiting.copy(phase = "ReleasingCarried", completion = Some(completion), calls = 4)
  private val releasing = releasingCarried.copy(phase = "Releasing", releasedCarried = 1, calls = 5)
  private val settling = releasing.copy(phase = "Settling", calls = 6)
  private val carrying = settling.copy(completion = Some(carryCompletion), carry = Some(carried))
  private val blocked = waiting.copy(phase = "Blocked", reason = Some("query timed out"),
    resumePhase = Some("WaitingChild"), reconciliations = 2)
  private val activeVariants = Vector(reserving, preparing, delivering, waiting, reconciling,
    releasingCarried, releasing, settling, carrying, blocked)

  private def roundtrip(value: AnyRef): AnyRef = {
    val serializer = serialization.findSerializerFor(value)
    serialization.deserialize(serialization.serialize(value).get, serializer.identifier,
      Serializers.manifestFor(serializer, value)).get
  }
  private def stateWith(batch: ActiveBatch): BatchState = BatchState(Some("job"), Some(definition.fingerprint),
    nextSequence = records.size, nextBatch = 1L, records = records.map(r => r.item.sequence ->
      r.copy(phase = "Running", batchId = Some(manifest.batchId))).toMap,
    batches = Map(manifest.batchId -> batch))
  private def settledRecords: Vector[ItemRecord] = records.zipWithIndex.map { case (r, index) => index match {
    case 0 => r.copy(phase = "Succeeded", result = Vector[Byte](0, -1, 42))
    case 1 => r.copy(phase = "Failed", reason = Some("declined"))
    case 2 => r.copy(dueAt = 7000L, item = r.item.copy(executions = Map("a" -> 2)))
    case 3 => r.copy(dueAt = 8000L, item = r.item.copy(area = "b", compatibilityKey = "routed", carried = Vector(carried)))
    case _ => r.copy(phase = "Held", reason = Some("needs verification"))
  }}

  "Batch protobuf" should {
    "pin every definition, policy, budget and shared limit with deterministic maps and ordered areas" in {
      val original = definition.fingerprint
      original should fullyMatch regex "[0-9a-f]{64}"
      val settingsChanges = Vector(
        settings.copy(maxAcceptedItems = 999), settings.copy(maxActiveItems = 99),
        settings.copy(maxActiveBytes = 512 * 1024), settings.copy(maxItemBytes = 4095),
        settings.copy(maxResultBytes = 4095), settings.copy(maxConcurrentBatches = 3),
        settings.copy(maxTransitions = 15), settings.copy(maxBatchCalls = 299),
        settings.copy(maxReconciliations = 2), settings.copy(pollIntervalMillis = 251),
        settings.copy(callTimeoutMillis = 5001))
      val l = settings.limits
      val limitChanges = Vector(l.copy(maxRequestBytes = 32768), l.copy(maxStateBytes = 512 * 1024),
        l.copy(maxResultBytes = 512 * 1024), l.copy(maxEvidenceBytes = 32768),
        l.copy(maxEnvelopeBytes = 2 * 1024 * 1024), l.copy(maxStringBytes = 4095),
        l.copy(maxPlanSteps = 1023), l.copy(maxMetadataEntries = 63))
      val areaChanges = Vector(area.copy(id = "a2"), area.copy(policyVersion = "policy-2"),
        area.copy(resources = Map("machine" -> 3, "operator" -> 1)), area.copy(parallelism = 3),
        area.copy(maxExecutions = 6), area.copy(batch = BatchPolicy.CountOrWait(9, 200, 2)),
        area.copy(batch = BatchPolicy.CountOrWait(10, 201, 2)),
        area.copy(batch = BatchPolicy.CountOrWait(10, 200, 1)),
        area.copy(batch = BatchPolicy.CountOrWait(10, 200, 2, allowPartialOnClose = false)))
      val changes = Vector(definition.copy(id = "other"), definition.copy(version = "v2"),
        definition.copy(areas = definition.areas.reverse)) ++
        areaChanges.map(a => definition.copy(areas = Vector(a, definition.areas(1)))) ++
        settingsChanges.map(s => definition.copy(settings = s)) ++
        limitChanges.map(changed => definition.copy(settings = settings.copy(limits = changed)))
      changes.foreach(d => withClue(d.id + " " + d.version) { d.fingerprint should not be original })
      definition.copy(areas = definition.areas.map(a => a.copy(resources =
        ListMap("operator" -> 1, "machine" -> 2)))).fingerprint shouldBe original
      definition.copy(id = "ab", version = "c").fingerprint should not be
        definition.copy(id = "a", version = "bc").fingerprint
    }

    "roundtrip public manifest and completion bytes without losing order, decisions or resource proof" in {
      BatchCodec.manifestFromBytes(BatchCodec.manifestToBytes(manifest)) shouldBe manifest
      BatchCodec.manifestFromBytes(BatchCodec.manifestToBytes(manifest.copy(items = items.reverse))).items shouldBe items.reverse
      BatchCodec.manifestToBytes(manifest) should not be BatchCodec.manifestToBytes(manifest.copy(items = items.reverse))
      Vector(completion, carryCompletion).foreach { c =>
        val encoded = BatchCodec.completionToBytes(c)
        val mutable = encoded.toArray
        val frozen = ByteString(mutable)
        mutable(0) = 0
        val decoded = BatchCodec.completionFromBytes(frozen)
        decoded shouldBe c
        decoded.decisions shouldBe c.decisions
        decoded.resource.proof shouldBe c.resource.proof
      }
      val reversedMaps = manifest.copy(items = items.map(i => i.copy(executions = ListMap("b" -> 2, "a" -> 1))))
      BatchCodec.manifestToBytes(reversedMaps) shouldBe BatchCodec.manifestToBytes(manifest)
    }

    "roundtrip every event and full snapshots including grants, delivery, retries, held items and carry" in {
      import BatchEvent._
      val events: Vector[BatchEvent] = Vector(Initialized("job", definition.fingerprint), InputAccepted(records.head),
        Sealed(reserving), Settled(manifest.batchId, settledRecords), Verified(settledRecords.last.copy(phase = "Pending")),
        InputClosed) ++ activeVariants.map(Advanced)
      events.foreach { event =>
        serialization.findSerializerFor(event).identifier shouldBe 77102160
        Serializers.manifestFor(serialization.findSerializerFor(event), event) shouldBe "BE1"
        roundtrip(event) shouldBe event
      }
      val terminal = BatchState(Some("job"), Some(definition.fingerprint), nextSequence = records.size,
        nextBatch = 1L, closed = true, records = settledRecords.map(r => r.item.sequence -> r).toMap)
      val states = Vector(BatchState(), BatchState(Some("job"), Some(definition.fingerprint)), terminal,
        terminal.copy(invalid = Some("invalid event frontier"))) ++ activeVariants.map(stateWith)
      states.foreach { state =>
        serialization.findSerializerFor(state).identifier shouldBe 77102160
        Serializers.manifestFor(serialization.findSerializerFor(state), state) shouldBe "BS1"
        roundtrip(state) shouldBe state
      }
      val decoded = roundtrip(stateWith(carrying)).asInstanceOf[BatchState].batches(manifest.batchId)
      decoded.grant shouldBe Some(grant)
      decoded.delivery.get.child shouldBe child
      decoded.delivery.get.payload shouldBe delivery.payload
      decoded.carry shouldBe Some(carried)
      decoded.completion shouldBe Some(carryCompletion)
      val reordered = stateWith(waiting).copy(records = ListMap(stateWith(waiting).records.toVector.reverse: _*))
      serialization.serialize(reordered).get.toVector shouldBe serialization.serialize(stateWith(waiting)).get.toVector
    }

    "allow a finite 1000-record history beyond the R1 envelope and a 100-item active batch" in {
      val history = Vector.tabulate(1000) { n =>
        val item = Item(n.toLong, s"item-$n-" + ("x" * 128), Vector.empty, "history", "a")
        n.toLong -> ItemRecord(item, "a" * 64, "Succeeded", 1L, 1L, result = Vector.fill[Byte](4096)(-1))
      }.toMap
      val state = BatchState(Some("job"), Some(definition.fingerprint), nextSequence = 1000,
        nextBatch = 1000, closed = true, records = history)
      val encoded = serialization.serialize(state).get
      encoded.length should be > (4 * 1024 * 1024)
      encoded.length should be < BatchCodec.MaxEnvelopeBytes
      roundtrip(state) shouldBe state
      val largeItems = Vector.tabulate(100)(n => Item(n.toLong, s"active-$n", Vector.fill[Byte](4096)(1), "key", "a"))
      val largeManifest = manifest.copy(items = largeItems)
      val active = BatchState(Some("job"), Some(definition.fingerprint), nextSequence = 100, nextBatch = 1,
        records = largeItems.map(i => i.sequence -> ItemRecord(i, "a" * 64, "Running", 1, 1,
          batchId = Some(manifest.batchId))).toMap,
        batches = Map(manifest.batchId -> ActiveBatch(largeManifest)))
      roundtrip(active) shouldBe active
      intercept[IllegalArgumentException](BatchCodec.definitionDigest(definition.copy(settings = settings.copy(maxActiveItems = 101))))
    }

    "roundtrip every public message and deliver through every restored reply reference" in {
      val replies = testKit.createTestProbe[Reply]()
      val itemViews = settledRecords.map(r => ItemView(r.item.sequence, r.item.itemId, r.item.area,
        r.phase, r.item.executions, r.reason, ByteString(r.result.toArray)))
      val view = View("job", "Open", inputClosed = false, accepted = 5, succeeded = 1, failed = 1, held = 1,
        active = 3, activeBytes = 9, batches = Vector.empty, items = itemViews, truncated = true, reason = Some("details"))
      val commands: Vector[Wire] = Vector(Offer(0, "item", ByteString(0, -1, 127), "a", "key", replies.ref),
        CloseInput(replies.ref), Get(replies.ref), Get(replies.ref, 12L, 7), Reconcile(manifest.batchId, replies.ref),
        VerifyItem(4, "b", "verify", replies.ref))
      val phases = Vector("Open", "Closing", "ClosingBlocked", "Drained", "Blocked", "RecoveryBlocked")
      val activeViews = activeVariants.map(b => Current(View("job", "Open", false, 5, 0, 0, 0, 5, 15,
        Vector(BatchView(manifest.batchId, "a", b.phase, items.map(_.itemId), b.grant.map(_.reservationId),
          b.delivery.map(_.deliveryId), b.reason)), Vector.empty, false, None)))
      val wire = commands ++ Vector(Accepted(0, "item", false), Accepted(0, "item", true),
        Backpressured("full"), Rejected("closed")) ++ phases.map(p => Current(view.copy(phase = p))) ++ activeViews
      wire.foreach { value =>
        serialization.findSerializerFor(value).identifier shouldBe 77102161
        Serializers.manifestFor(serialization.findSerializerFor(value), value) shouldBe "BW1"
        roundtrip(value) shouldBe value
      }
      commands.foreach { command =>
        val ref = roundtrip(command).asInstanceOf[Wire] match {
          case o: Offer => o.replyTo
          case c: CloseInput => c.replyTo
          case g: Get => g.replyTo
          case r: Reconcile => r.replyTo
          case v: VerifyItem => v.replyTo
          case other => fail(s"unexpected command $other")
        }
        ref ! Accepted(1L, "delivered", false)
        replies.expectMessage(Accepted(1L, "delivered", false))
      }
      serialization.serialize(Drive(1L, 1L)).isFailure shouldBe true
      serialization.serialize(CallTimeout(1L, manifest.batchId, 1L)).isFailure shouldBe true
      serialization.serialize(Called(1L, manifest.batchId, 1L, Success(ResourceResult(ResourceProtocol.Queued("r"))))).isFailure shouldBe true
      ConfigFactory.parseResources("batch-reference.conf").getConfig("akka.actor.serialization-bindings")
        .root().containsKey(classOf[Command].getName) shouldBe false
    }

    "reject unknown schemas, alternatives, fields, status enums and missing nested values" in {
      val serializer = serialization.findSerializerFor(BatchState()).asInstanceOf[BatchSerializer]
      Vector(BatchEventPO(schemaVersion = 2).toByteArray, BatchEventPO(schemaVersion = 1).toByteArray,
        Array[Byte](8, 1, -94, 6, 0)).foreach(bytes => intercept[IllegalArgumentException](serializer.fromBinary(bytes, "BE1")))
      intercept[IllegalArgumentException](serializer.fromBinary(BatchStatePO(schemaVersion = 2).toByteArray, "BS1"))
      intercept[IllegalArgumentException](serializer.fromBinary(Array.emptyByteArray, "future"))
      intercept[IllegalArgumentException](serializer.fromBinary(new Array[Byte](BatchCodec.MaxEnvelopeBytes + 1), "BE1"))
      val record = BatchCodec.recordToProto(records.head)
      val badRecord = record.copy(phase = BatchItemPhasePO.Unrecognized(99))
      intercept[IllegalArgumentException](BatchCodec.eventFromProto(BatchEventPO(schemaVersion = 1,
        event = BatchEventPO.Event.InputAccepted(badRecord))))
      val active = BatchCodec.activeToProto(waiting)
      Vector(active.copy(phase = BatchActivePhasePO.Unrecognized(99)), active.copy(manifest = None),
        active.copy(grant = None), active.copy(delivery = None),
        active.copy(resumePhase = Some(BatchActivePhasePO.Unrecognized(99)))).foreach { p =>
        intercept[IllegalArgumentException](BatchCodec.eventFromProto(BatchEventPO(schemaVersion = 1,
          event = BatchEventPO.Event.Advanced(p))))
      }
      val unknownNested = BatchManifestPO.parseFrom(BatchCodec.manifestToBytes(manifest).toArray ++ Array[Byte](-94, 6, 0))
      intercept[IllegalArgumentException](BatchCodec.manifestFromProto(unknownNested))
      intercept[IllegalArgumentException](BatchCodec.completionFromProto(BatchCompletionPO(schemaVersion = 1)))
      val cp = BatchCodec.completionToProto(completion)
      intercept[IllegalArgumentException](BatchCodec.completionFromProto(cp.copy(decisions = cp.decisions.updated(0,
        cp.decisions.head.copy(disposition = BatchDecisionPO.Disposition.Empty)))))
      intercept[IllegalArgumentException](BatchCodec.completionFromBytes(ByteString(new Array[Byte](BatchCodec.MaxValueBytes + 1))))
      intercept[IllegalArgumentException](BatchCodec.manifestFromBytes(ByteString(new Array[Byte](BatchCodec.MaxValueBytes + 1))))
    }

    "reject malformed counts, duplicate keys and ids, mismatched frontiers and negative times in both directions" in {
      val state = stateWith(waiting)
      val po = BatchCodec.stateToProto(state)
      Vector(po.copy(records = po.records :+ po.records.head), po.copy(batches = po.batches :+ po.batches.head),
        po.copy(nextSequence = -1), po.copy(nextBatch = -1), po.copy(nextSequence = 1001),
        po.copy(records = Vector.fill(1001)(po.records.head)),
        po.copy(records = po.records.updated(0, po.records.head.copy(sequence = 99)))).foreach { p =>
        intercept[IllegalArgumentException](BatchCodec.stateFromProto(p))
      }
      val mp = BatchCodec.manifestToProto(manifest)
      Vector(mp.copy(items = mp.items :+ mp.items.head), mp.copy(items = Vector.fill(101)(mp.items.head)),
        mp.copy(sealedAt = -1), mp.copy(items = mp.items.updated(0, mp.items.head.copy(sequence = -1))),
        mp.copy(items = mp.items.updated(0, mp.items.head.copy(executions =
          mp.items.head.executions :+ mp.items.head.executions.head)))).foreach { p =>
        intercept[IllegalArgumentException](BatchCodec.manifestFromProto(p))
      }
      val ap = BatchCodec.activeToProto(waiting)
      Vector(ap.copy(calls = -1), ap.copy(reconciliations = -1), ap.copy(releasedCarried = 2),
        ap.copy(nextDue = -1)).foreach(p => intercept[IllegalArgumentException](BatchCodec.eventFromProto(
        BatchEventPO(schemaVersion = 1, event = BatchEventPO.Event.Advanced(p)))))
      val rp = BatchCodec.recordToProto(records.head)
      Vector(rp.copy(acceptedAt = -1), rp.copy(dueAt = -1)).foreach(p => intercept[IllegalArgumentException](
        BatchCodec.eventFromProto(BatchEventPO(schemaVersion = 1, event = BatchEventPO.Event.InputAccepted(p)))))
      serialization.serialize(state.copy(records = state.records.updated(0L,
        state.records(0L).copy(item = state.records(0L).item.copy(itemId = "item-1"))))).isFailure shouldBe true
      serialization.serialize(BatchEvent.Advanced(waiting.copy(nextDue = -1))).isFailure shouldBe true
      serialization.serialize(BatchEvent.InputAccepted(records.head.copy(dueAt = -1))).isFailure shouldBe true
      serialization.serialize(BatchEvent.Settled(manifest.batchId, Vector(records.head, records.head))).isFailure shouldBe true
      intercept[IllegalArgumentException](BatchCodec.completionToBytes(completion.copy(decisions = completion.decisions :+ completion.decisions.head)))
      intercept[IllegalArgumentException](BatchCodec.completionToBytes(completion.copy(decisions =
        Vector(ItemDecision("item", ItemDisposition.Retry(-1))))))
      intercept[IllegalArgumentException](BatchCodec.manifestToBytes(manifest.copy(items = items.updated(0,
        items.head.copy(payload = Vector.fill[Byte](4097)(1))))))
      intercept[IllegalArgumentException](BatchCodec.manifestToBytes(manifest.copy(items = items.updated(0,
        items.head.copy(lineage = Vector.fill(33)("batch"))))))
      val duplicateUnits = BatchCodec.completionToProto(carryCompletion)
      val carry = duplicateUnits.resource.asInstanceOf[BatchCompletionPO.Resource.Carry].value
      intercept[IllegalArgumentException](BatchCodec.completionFromProto(duplicateUnits.copy(resource =
        BatchCompletionPO.Resource.Carry(carry.copy(units = carry.units :+ carry.units.head)))))
    }

    "bound public offer bytes, UTF-8 identities, page sizes, view counts and wire envelopes" in {
      val replies = testKit.createTestProbe[Reply]()
      val offer = Offer(0, "item", ByteString.empty, "a", "key", replies.ref)
      val serializer = serialization.findSerializerFor(offer).asInstanceOf[BatchWireSerializer]
      Vector(offer.copy(sequence = -1), offer.copy(payload = ByteString(new Array[Byte](4097))),
        offer.copy(itemId = "界" * 1366), offer.copy(compatibilityKey = ""), offer.copy(replyTo = null)).foreach { p =>
        serialization.serialize(p).isFailure shouldBe true
      }
      Vector(Get(replies.ref, -2, 1), Get(replies.ref, -1, 0), Get(replies.ref, -1, 101)).foreach { p =>
        serialization.serialize(p).isFailure shouldBe true
      }
      val row = ItemView(0, "item", "a", "Pending", Map.empty, None, ByteString.empty)
      val view = View("job", "Open", false, 1, 0, 0, 0, 1, 0, Vector.empty, Vector(row), false, None)
      Vector(view.copy(items = Vector.fill(101)(row)), view.copy(items = Vector(row, row)),
        view.copy(accepted = -1), view.copy(active = 2), view.copy(succeeded = 2),
        view.copy(activeBytes = -1), view.copy(phase = "unrecognized"),
        view.copy(items = Vector(row.copy(phase = "unrecognized")))).foreach { v =>
        serialization.serialize(Current(v)).isFailure shouldBe true
      }
      val vp = BatchCodec.viewToProto(view)
      Vector(BatchWirePO(schemaVersion = 1), BatchWirePO(schemaVersion = 2),
        BatchWirePO(schemaVersion = 1, message = BatchWirePO.Message.Current(vp.copy(phase = BatchJobPhasePO.Unrecognized(99))))).foreach { p =>
        intercept[IllegalArgumentException](serializer.fromBinary(p.toByteArray, "BW1"))
      }
      val path = akka.actor.typed.ActorRefResolver(system).toSerializationFormat(replies.ref)
      intercept[IllegalArgumentException](serializer.fromBinary(BatchWirePO(schemaVersion = 1,
        message = BatchWirePO.Message.Get(BatchGetPO(replyTo = path, afterSequence = -1, limit = 101))).toByteArray, "BW1"))
      intercept[IllegalArgumentException](serializer.fromBinary(BatchWirePO(schemaVersion = 1,
        message = BatchWirePO.Message.Offer(BatchOfferPO(sequence = -1, itemId = "item", area = "a",
          compatibilityKey = "key", replyTo = path))).toByteArray, "BW1"))
      intercept[IllegalArgumentException](serializer.fromBinary(new Array[Byte](BatchCodec.MaxWireBytes + 1), "BW1"))
      intercept[IllegalArgumentException](serializer.fromBinary(Array.emptyByteArray, "future"))
    }
  }
}
