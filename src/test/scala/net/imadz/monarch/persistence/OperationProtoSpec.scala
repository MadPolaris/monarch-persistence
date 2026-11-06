package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.serialization.{SerializationExtension, Serializers}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.operation._
import net.imadz.monarch.persistence.operation.OperationProtocol._
import net.imadz.monarch.persistence.operation.proto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class OperationProtoSpec extends ScalaTestWithActorTestKit(
    ConfigFactory.parseString("akka.actor.allow-java-serialization = off")
      .withFallback(ConfigFactory.parseResources("operation-reference.conf")))
    with AnyWordSpecLike with Matchers {
  private val serialization = SerializationExtension(system.classicSystem)
  private val limits = OperationSettings().limits
  private val policy = OperationSettings(maxChecks = 2, manualCheckBudget = 1, maxReconciliations = 1,
    maxSubmissions = 2).policy
  private val request = Request("op", "effect", "step#0", "bank", "1", "durable-ledger-7",
    ByteString(0, 1, -1, 127), Some("reservation"))
  private val proof = Evidence(request.operationKey, request.fingerprint, request.ledgerEpoch,
    Some("task-3"), ByteString(127, 0, -1))
  private val success = Succeeded(ByteString(0, -1, 42), proof)
  private val failure = KnownFailed("DECLINED", "authoritative business rejection", ByteString(1, 0), proof)

  private def roundtrip(value: AnyRef): AnyRef = {
    val serializer = serialization.findSerializerFor(value)
    serialization.deserialize(serialization.serialize(value).get, serializer.identifier,
      Serializers.manifestFor(serializer, value)).get
  }

  "Operation protobuf" should {
    "frame all immutable request fields without concatenation or option aliases" in {
      val array = Array[Byte](0, 1, -1)
      val original = request.copy(payload = ByteString(array), reservationId = None)
      val digest = original.fingerprint
      array(0) = 42
      original.payload shouldBe ByteString(0, 1, -1)
      original.fingerprint shouldBe digest
      Vector(original.copy(operationKey = "other"), original.copy(effectKey = "other"),
        original.copy(stepId = "other"), original.copy(connectorId = "other"),
        original.copy(connectorVersion = "2"), original.copy(ledgerEpoch = "other"),
        original.copy(payload = ByteString(1)), original.copy(reservationId = Some("")),
        original.copy(reservationId = Some("reservation"))).foreach(_.fingerprint should not be digest)
      original.copy(effectKey = "ab", stepId = "c").fingerprint should not be
        original.copy(effectKey = "a", stepId = "bc").fingerprint
    }

    "roundtrip every event and complete durable frontier with its own serializer id" in {
      import OperationEvent._
      val events: Vector[OperationEvent] = Vector(
        Prepared(request, request.fingerprint, policy), DispatchStarted(1, None),
        DispatchStarted(2, Some(proof)), QueryStarted(1),
        RunningObserved(proof, Some(100L)), RunningObserved(proof, None),
        UnknownObserved("unavailable", Some(100L)), UnknownObserved("budget exhausted", None),
        Blocked("bad proof"), Reconciled(1, 1, 100L),
        TerminalRecorded(success), TerminalRecorded(failure))
      events.foreach { event =>
        serialization.findSerializerFor(event).identifier shouldBe 77102130
        roundtrip(event) shouldBe event
      }
      val empty = OperationState()
      val prepared = OperationState.evolve(empty, Prepared(request, request.fingerprint, policy))
      val dispatching = OperationState.evolve(prepared, DispatchStarted(1, None))
      val running = OperationState.evolve(dispatching, RunningObserved(proof, Some(100L)))
      val querying = OperationState.evolve(running, QueryStarted(1))
      val unknown = OperationState.evolve(querying, UnknownObserved("not found is not safe", Some(200L)))
      val blocked = OperationState.evolve(querying, Blocked("external call is outstanding"))
      val manual = OperationState.evolve(blocked, Reconciled(1, 1, 200L))
      val succeeded = OperationState.evolve(querying, TerminalRecorded(success))
      val failed = OperationState.evolve(querying, TerminalRecorded(failure))
      Vector(empty, prepared, dispatching, running, querying, unknown, blocked, manual, succeeded, failed).foreach { state =>
        state.invalid shouldBe None
        serialization.findSerializerFor(state).identifier shouldBe 77102130
        roundtrip(state) shouldBe state
      }
      val conflict = OperationState.evolve(succeeded, TerminalRecorded(failure))
      conflict.terminal shouldBe Some(success)
      conflict.phase shouldBe Phase.Succeeded
      conflict.invalid should not be empty
      OperationState.recoveryProblem(prepared.copy(version = 2), request.operationKey,
        request.connectorId, request.connectorVersion, limits) should not be empty
    }

    "roundtrip bytes wire DTOs and preserve usable reply actor references" in {
      val replies = testKit.createTestProbe[Reply]()
      val empty = View(request.operationKey, None, Phase.Empty, None, None, None, 0)
      val views = Vector(empty,
        View(request.operationKey, Some(request.fingerprint), Phase.Prepared, None, None, None, 0),
        View(request.operationKey, Some(request.fingerprint), Phase.Dispatching, None, None, None, 0),
        View(request.operationKey, Some(request.fingerprint), Phase.Ongoing, None, None, Some(100L), 1),
        View(request.operationKey, Some(request.fingerprint), Phase.Reconciling, None, Some("unknown"), Some(100L), 1),
        View(request.operationKey, Some(request.fingerprint), Phase.Blocked, None, Some("exhausted"), None, 2),
        View(request.operationKey, Some(request.fingerprint), Phase.Succeeded, Some(success), None, None, 2),
        View(request.operationKey, Some(request.fingerprint), Phase.Failed, Some(failure), None, None, 2),
        View(request.operationKey, Some(request.fingerprint), Phase.RecoveryBlocked, None, Some("version"), None, 2))
      val wire: Vector[Wire] = Vector(Ensure(request, replies.ref), Get(request.operationKey, replies.ref),
        Reconcile(request.operationKey, replies.ref), Rejected(request.operationKey, "conflict")) ++ views.map(Current)
      wire.foreach { value =>
        serialization.findSerializerFor(value).identifier shouldBe 77102131
        roundtrip(value) shouldBe value
      }
      val decodedEnsure = roundtrip(Ensure(request, replies.ref)).asInstanceOf[Ensure]
      decodedEnsure.replyTo ! Current(empty)
      replies.expectMessage(Current(empty))
      roundtrip(Get(request.operationKey, replies.ref)).asInstanceOf[Get].replyTo ! Current(empty)
      replies.expectMessage(Current(empty))
      roundtrip(Reconcile(request.operationKey, replies.ref)).asInstanceOf[Reconcile].replyTo ! Current(empty)
      replies.expectMessage(Current(empty))
      serialization.serialize(Drive(1L)).isFailure shouldBe true
      serialization.serialize(CallTimedOut(1L, 1L)).isFailure shouldBe true
    }

    "reject unknown schemas, oneofs, statuses, malformed identities and oversized envelopes" in {
      val serializer = serialization.findSerializerFor(OperationState()).asInstanceOf[OperationSerializer]
      intercept[IllegalArgumentException](serializer.fromBinary(
        OperationEventPO(schemaVersion = 2).toByteArray, "OE1"))
      intercept[IllegalArgumentException](serializer.fromBinary(
        OperationEventPO(schemaVersion = 1).toByteArray, "OE1"))
      // schema=1 plus unknown length-delimited oneof field 100.
      intercept[IllegalArgumentException](serializer.fromBinary(Array[Byte](8, 1, -94, 6, 0), "OE1"))
      val validState = OperationProtoConverters.stateToProto(OperationState())
      intercept[IllegalArgumentException](serializer.fromBinary(
        validState.copy(phase = OperationPhasePO.Unrecognized(99)).toByteArray, "OS1"))
      intercept[IllegalArgumentException](serializer.fromBinary(validState.copy(stateVersion = 2).toByteArray, "OS1"))
      intercept[IllegalArgumentException](serializer.fromBinary(new Array[Byte](limits.maxEnvelopeBytes + 1), "OE1"))
      intercept[IllegalArgumentException](serializer.fromBinary(Array.emptyByteArray, "future-manifest"))
      serialization.serialize(OperationEvent.Prepared(request, "0" * 64, policy)).isFailure shouldBe true
      serialization.serialize(OperationEvent.QueryStarted(-1)).isFailure shouldBe true
      serialization.serialize(OperationEvent.TerminalRecorded(success.copy(
        result = ByteString(new Array[Byte](limits.maxResultBytes + 1))))).isFailure shouldBe true
      serialization.serialize(OperationEvent.RunningObserved(proof.copy(
        payload = ByteString(new Array[Byte](limits.maxEvidenceBytes + 1))), Some(1L))).isFailure shouldBe true

      val reply = testKit.createTestProbe[Reply]()
      val wireSerializer = serialization.findSerializerFor(Get("op", reply.ref)).asInstanceOf[OperationWireSerializer]
      intercept[IllegalArgumentException](wireSerializer.fromBinary(OperationWirePO(schemaVersion = 1).toByteArray, "OW1"))
      intercept[IllegalArgumentException](wireSerializer.fromBinary(
        new Array[Byte](limits.maxEnvelopeBytes + 1), "OW1"))
      val unknownView = OperationViewPO(operationKey = "op", phase = OperationPhasePO.Unrecognized(99))
      intercept[IllegalArgumentException](wireSerializer.fromBinary(OperationWirePO(schemaVersion = 1,
        message = OperationWirePO.Message.Current(OperationCurrentPO(view = Some(unknownView)))).toByteArray, "OW1"))
      serialization.serialize(Ensure(request.copy(payload = ByteString(new Array[Byte](limits.maxRequestBytes + 1))),
        reply.ref)).isFailure shouldBe true
      serialization.serialize(Ensure(request.copy(effectKey = "界" * (limits.maxStringBytes / 3 + 1)),
        reply.ref)).isFailure shouldBe true
      serialization.serialize(Current(View("op", Some(request.fingerprint), "Idle", None, None, None, 0))).isFailure shouldBe true
      serialization.serialize(Ensure(request, null)).isFailure shouldBe true
    }

    "reject forged ongoing snapshots and resubmission history without a query" in {
      import OperationEvent._
      val prepared = OperationState.evolve(OperationState(), Prepared(request, request.fingerprint, policy))
      val dispatched = OperationState.evolve(prepared, DispatchStarted(1, None))
      val forged = dispatched.copy(phase = Phase.Ongoing, nextCheckAt = Some(1L), evidence = None)
      OperationValidation.state(forged, limits) should not be empty
      serialization.serialize(forged).isFailure shouldBe true
      val replayed = OperationState.evolve(dispatched, DispatchStarted(2, Some(proof)))
      replayed.invalid should not be empty
      replayed.submissions shouldBe 1
      val queried = OperationState.evolve(dispatched, QueryStarted(1))
      OperationState.evolve(queried, DispatchStarted(2, Some(proof))).invalid shouldBe None
    }

    "validate positive durations, finite grants and framing capacity in configuration" in {
      val defaults = ConfigFactory.parseResources("operation-reference.conf")
      OperationSettings.fromConfig(defaults) shouldBe OperationSettings()
      Vector("max-checks = 0", "manual-check-budget = 0", "max-reconciliations = -1", "max-submissions = 0",
        "io-timeout = 0ms", "check-interval = 8d", "limits.max-evidence-bytes = 0",
        "limits.max-string-bytes = 63", "limits.max-envelope-bytes = 1").foreach { field =>
        intercept[IllegalArgumentException](OperationSettings.fromConfig(
          ConfigFactory.parseString(s"monarch.operation.$field").withFallback(defaults)))
      }
      intercept[IllegalArgumentException](OperationSettings(maxChecks = Int.MaxValue, manualCheckBudget = 1))
      intercept[IllegalArgumentException](OperationSettings(limits = ProtocolLimits(
        maxRequestBytes = 10, maxStateBytes = 10, maxResultBytes = 10, maxEvidenceBytes = 10,
        maxEnvelopeBytes = 100, maxStringBytes = 64)))
    }
  }
}
