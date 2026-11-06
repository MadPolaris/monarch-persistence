package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.serialization.{SerializationExtension, Serializers}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.resource._
import net.imadz.monarch.persistence.resource.ResourceProtocol._
import net.imadz.monarch.persistence.resource.proto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ResourceProtoSpec extends ScalaTestWithActorTestKit(
    ConfigFactory.parseString("akka.actor.allow-java-serialization = off")
      .withFallback(ConfigFactory.parseResources("resource-reference.conf")))
    with AnyWordSpecLike with Matchers {
  private val serialization = SerializationExtension(system.classicSystem)
  private val definition = ResourceDefinition("proto-domain", Map("execution" -> 1, "slot" -> 1), ResourceSettings())
  private val request = Request("transport", "transport-owner", Map("execution" -> 1, "slot" -> 1))
  private val grant = ResourceIdentity.grant(definition, request)
  private val proof = Proof(request.ownerOperationKey, ByteString(0, -1, 42))
  private val targetRequest = Request("arrived", "occupancy-owner", Map("slot" -> 1))
  private val target = ResourceIdentity.grant(definition, targetRequest)
  private val targetProof = Proof(target.ownerOperationKey, ByteString(127, 0, -1))
  private val waiting = Request("waiting", "waiting-owner", Map("slot" -> 1))

  private def roundtrip(value: AnyRef): AnyRef = {
    val serializer = serialization.findSerializerFor(value)
    serialization.deserialize(serialization.serialize(value).get, serializer.identifier,
      Serializers.manifestFor(serializer, value)).get
  }

  "Resource protobuf" should {
    "frame every identity field deterministically without map-order or concatenation aliases" in {
      request.copy(units = Map("slot" -> 1, "execution" -> 1)).fingerprint shouldBe request.fingerprint
      request.copy(reservationId = "other").fingerprint should not be request.fingerprint
      request.copy(ownerOperationKey = "other").fingerprint should not be request.fingerprint
      request.copy(units = Map("execution" -> 1)).fingerprint should not be request.fingerprint
      request.copy(reservationId = "ab", ownerOperationKey = "c").fingerprint should not be
        request.copy(reservationId = "a", ownerOperationKey = "bc").fingerprint
      ResourceIdentity.grant(definition.copy(coordinatorId = "other-domain"), request).token should not be grant.token
      ResourceIdentity.grant(definition, request).token shouldBe grant.token
      val array = Array[Byte](0, -1, 42)
      val immutable = Proof(request.ownerOperationKey, ByteString(array))
      array(0) = 100
      immutable.evidence shouldBe proof.evidence
      ResourceIdentity.release(request.reservationId, grant.token, immutable) shouldBe
        ResourceIdentity.release(request.reservationId, grant.token, proof)
    }

    "roundtrip every event and queued, active, released and transferred snapshot frontier" in {
      import ResourceEvent._
      val defined = Defined(definition, definition.fingerprint)
      val events: Vector[ResourceEvent] = Vector(defined, GrantRecorded(grant), Enqueued(waiting),
        TransferRecorded(grant.reservationId, grant.token, target, proof),
        ReleaseRecorded(target.reservationId, target.token, targetProof),
        GrantRecorded(ResourceIdentity.grant(definition, waiting)),
        ReleaseRecorded(waiting.reservationId, ResourceIdentity.grant(definition, waiting).token,
          Proof(waiting.ownerOperationKey, ByteString("done"))))
      events.foreach { event =>
        serialization.findSerializerFor(event).identifier shouldBe 77102140
        Serializers.manifestFor(serialization.findSerializerFor(event), event) shouldBe "RE1"
        roundtrip(event) shouldBe event
      }
      val states = events.scanLeft(ResourceState())(ResourceState.evolve)
      states.foreach { state =>
        state.invalid shouldBe None
        serialization.findSerializerFor(state).identifier shouldBe 77102140
        Serializers.manifestFor(serialization.findSerializerFor(state), state) shouldBe "RS1"
        roundtrip(state) shouldBe state
      }
      states(4).used shouldBe Map("slot" -> 1L) // Transfer released execution, not destination occupancy.
      states.last.used shouldBe empty
      states.last.records(grant.reservationId).closure.get.targetReservationId shouldBe Some(target.reservationId)
      states.last.records(target.reservationId).closure.get.targetReservationId shouldBe None
    }

    "roundtrip every wire alternative and preserve usable reply actor references" in {
      val replies = testKit.createTestProbe[Reply]()
      val commands: Vector[Wire] = Vector(Reserve(request, replies.ref), Query(request.reservationId, replies.ref),
        Release(grant.reservationId, grant.token, proof, replies.ref),
        Transfer(grant.reservationId, grant.token, target.reservationId, target.ownerOperationKey,
          target.units, proof, replies.ref))
      val wire = commands ++ Vector[Wire](Granted(grant), Queued("queued"), Released("released"),
        Rejected("conflict", "immutable request differs"), Unknown("unknown", "coordinator unavailable"))
      wire.foreach { value =>
        serialization.findSerializerFor(value).identifier shouldBe 77102141
        Serializers.manifestFor(serialization.findSerializerFor(value), value) shouldBe "RW1"
        roundtrip(value) shouldBe value
      }
      commands.foreach { value =>
        val to = roundtrip(value) match {
          case Reserve(_, ref) => ref
          case Query(_, ref) => ref
          case Release(_, _, _, ref) => ref
          case Transfer(_, _, _, _, _, _, ref) => ref
          case other => fail(s"not a decoded resource command: $other")
        }
        to ! Queued("usable-ref")
        replies.expectMessage(Queued("usable-ref"))
      }
      serialization.serialize(Drive).isFailure shouldBe true
    }

    "reject unknown schemas, missing alternatives, duplicate IDs and malformed nested values" in {
      val persistence = serialization.findSerializerFor(ResourceState()).asInstanceOf[ResourceSerializer]
      intercept[IllegalArgumentException](persistence.fromBinary(ResourceEventPO(schemaVersion = 2).toByteArray, "RE1"))
      intercept[IllegalArgumentException](persistence.fromBinary(ResourceEventPO(schemaVersion = 1).toByteArray, "RE1"))
      // schema=1 followed by unknown oneof field 100.
      intercept[IllegalArgumentException](persistence.fromBinary(Array[Byte](8, 1, -94, 6, 0), "RE1"))
      intercept[IllegalArgumentException](persistence.fromBinary(Array.emptyByteArray, "not-a-resource-manifest"))
      val empty = ResourceProtoConverters.stateToProto(ResourceState())
      intercept[IllegalArgumentException](persistence.fromBinary(empty.copy(stateVersion = 2).toByteArray, "RS1"))
      val active = ResourceState.evolve(ResourceState.evolve(ResourceState(),
        ResourceEvent.Defined(definition, definition.fingerprint)), ResourceEvent.GrantRecorded(grant))
      val encoded = ResourceProtoConverters.stateToProto(active)
      intercept[IllegalArgumentException](persistence.fromBinary(encoded.copy(
        records = encoded.records ++ encoded.records).toByteArray, "RS1"))
      val unit = ResourceUnitPO(resource = "slot", units = 1)
      val duplicate = ResourceRequestPO(reservationId = "bad", ownerOperationKey = "owner", units = Vector(unit, unit))
      intercept[IllegalArgumentException](persistence.fromBinary(ResourceEventPO(schemaVersion = 1,
        event = ResourceEventPO.Event.Enqueued(duplicate)).toByteArray, "RE1"))
      serialization.serialize(ResourceEvent.Defined(definition, "0" * 64)).isFailure shouldBe true
      serialization.serialize(ResourceEvent.GrantRecorded(grant.copy(token = "caller-chosen"))).isFailure shouldBe true
      serialization.serialize(ResourceEvent.Enqueued(request.copy(units = Map("slot" -> 0)))).isFailure shouldBe true
      serialization.serialize(ResourceEvent.Enqueued(request.copy(units = Map("slot" -> -1)))).isFailure shouldBe true
      serialization.serialize(ResourceEvent.Enqueued(request.copy(units = Map.empty))).isFailure shouldBe true
      val replies = testKit.createTestProbe[Reply]()
      val wire = serialization.findSerializerFor(Query("test", replies.ref)).asInstanceOf[ResourceWireSerializer]
      intercept[IllegalArgumentException](wire.fromBinary(ResourceWirePO(schemaVersion = 1).toByteArray, "RW1"))
      intercept[IllegalArgumentException](wire.fromBinary(ResourceWirePO(schemaVersion = 2).toByteArray, "RW1"))
      intercept[IllegalArgumentException](wire.fromBinary(Array.emptyByteArray, "RE1"))
      serialization.serialize(Reserve(request, null)).isFailure shouldBe true
    }

    "enforce string, count, request, evidence and envelope aggregate byte bounds" in {
      val limits = ProtocolLimits()
      val replies = testKit.createTestProbe[Reply]()
      serialization.serialize(Reserve(request.copy(ownerOperationKey = "界" * (limits.maxStringBytes / 3 + 1)), replies.ref))
        .isFailure shouldBe true
      serialization.serialize(Reserve(request.copy(units = (0 to limits.maxMetadataEntries).map(i => s"r$i" -> 1).toMap), replies.ref))
        .isFailure shouldBe true
      // Each key fits and entry count fits, but their aggregate request does not.
      val largeKeys = (0 until 20).map(i => (s"$i-" + "x" * 4000) -> 1).toMap
      serialization.serialize(Reserve(request.copy(units = largeKeys), replies.ref)).isFailure shouldBe true
      serialization.serialize(Release(grant.reservationId, grant.token,
        proof.copy(evidence = ByteString(new Array[Byte](limits.maxEvidenceBytes + 1))), replies.ref)).isFailure shouldBe true
      val persistence = serialization.findSerializerFor(ResourceState()).asInstanceOf[ResourceSerializer]
      intercept[IllegalArgumentException](persistence.fromBinary(new Array[Byte](limits.maxEnvelopeBytes + 1), "RE1"))
      val wire = serialization.findSerializerFor(Query("test", replies.ref)).asInstanceOf[ResourceWireSerializer]
      intercept[IllegalArgumentException](wire.fromBinary(new Array[Byte](limits.maxEnvelopeBytes + 1), "RW1"))
    }

    "fail closed on corrupt transitions or overallocated snapshots and reserve release tombstone bytes" in {
      import ResourceEvent._
      val initial = ResourceState.evolve(ResourceState(), Defined(definition, definition.fingerprint))
      val active = ResourceState.evolve(initial, GrantRecorded(grant))
      val duplicate = ResourceState.evolve(active, GrantRecorded(grant))
      duplicate.invalid should not be empty
      duplicate.used shouldBe active.used
      val badRelease = ResourceState.evolve(active, ReleaseRecorded(grant.reservationId, grant.token,
        proof.copy(operationKey = "wrong-owner")))
      badRelease.invalid should not be empty
      badRelease.used shouldBe active.used
      val forged = active.copy(records = active.records.updated(waiting.reservationId, ResourceRecord(waiting, granted = true)))
      ResourceValidation.state(forged) should not be empty
      serialization.serialize(forged).isFailure shouldBe true
      val badTarget = ResourceIdentity.grant(definition, targetRequest.copy(units = Map("execution" -> 1, "slot" -> 1)))
      val transferred = ResourceState.evolve(active, TransferRecorded(grant.reservationId, grant.token, target, proof))
      val conflict = ResourceState.evolve(transferred, TransferRecorded(grant.reservationId, grant.token, badTarget, proof))
      conflict.invalid should not be empty
      conflict.used shouldBe transferred.used

      val small = ResourceSettings(limits = ProtocolLimits(maxRequestBytes = 512, maxStateBytes = 2048,
        maxResultBytes = 512, maxEvidenceBytes = 128, maxEnvelopeBytes = 4096,
        maxStringBytes = 128, maxPlanSteps = 16, maxMetadataEntries = 4))
      val d = ResourceDefinition("bounded", Map("slot" -> 100), small)
      var state = ResourceState.evolve(ResourceState(), Defined(d, d.fingerprint))
      var full = false
      var count = 0
      while (!full && count < 100) {
        val r = Request(s"record-$count", s"owner-$count", Map("slot" -> 1))
        val next = ResourceState.evolve(state, GrantRecorded(ResourceIdentity.grant(d, r)))
        if (next.invalid.nonEmpty) full = true else state = next
        count += 1
      }
      full shouldBe true
      state.records.size should be < small.maxReservations
      // Exhausted aggregate admission must still leave room to release every live claim.
      state.records.values.toVector.foreach { r =>
        val g = ResourceIdentity.grant(d, r.request)
        state = ResourceState.evolve(state, ReleaseRecorded(g.reservationId, g.token, Proof(g.ownerOperationKey, ByteString("done"))))
        state.invalid shouldBe None
      }
      state.used shouldBe empty
      roundtrip(state) shouldBe state
    }

    "load bounded defaults and reject illegal configuration" in {
      ResourceSettings.fromConfig(ConfigFactory.parseResources("resource-reference.conf")) shouldBe ResourceSettings()
      intercept[IllegalArgumentException](ResourceSettings(maxReservations = 0))
      intercept[IllegalArgumentException](ResourceSettings(maxReservations = 10001))
      intercept[IllegalArgumentException](ResourceSettings(maxQueued = -1))
      intercept[IllegalArgumentException](ResourceSettings(maxQueued = 1001))
      intercept[IllegalArgumentException](ResourceSettings(limits = ProtocolLimits(maxStringBytes = 63)))
    }
  }
}
