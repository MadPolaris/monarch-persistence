package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.serialization.{SerializationExtension, Serializers}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.delivery._
import net.imadz.monarch.persistence.delivery.DeliveryProtocol._
import net.imadz.monarch.persistence.delivery.proto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try

class DeliveryProtoSpec extends ScalaTestWithActorTestKit(
    ConfigFactory.parseString("akka.actor.allow-java-serialization = off")
      .withFallback(ConfigFactory.parseResources("delivery-reference.conf")))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: ExecutionContext = system.executionContext
  private val serialization = SerializationExtension(system.classicSystem)
  private val limits = DeliverySettings().limits
  private val policy = DeliverySettings(maxPolls = 2).policy
  private val child = RunSubmission("child", "definition", "1", "codec-1", ByteString(0, -1, 3))
  private val request = Request("delivery", "parent", "target", child, ByteString(127, 0, -1))
  private val identity = RunProtocol.Identity(child.runId, child.definitionId, child.definitionVersion,
    child.codecVersion, ProtocolDigest.sha256(child.initialState), ProtocolDigest.fields(Vector(ByteString("step#0"))))
  private val result = ByteString(0, -1, 42)

  private def roundtrip(value: AnyRef): AnyRef = {
    val serializer = serialization.findSerializerFor(value)
    serialization.deserialize(serialization.serialize(value).get, serializer.identifier,
      Serializers.manifestFor(serializer, value)).get
  }

  "Delivery protobuf" should {
    "fingerprint the entire immutable request without field-boundary or caller-array aliases" in {
      val payload = Array[Byte](0, 1, -1)
      val initial = Array[Byte](1, 0, -1)
      val original = request.copy(child = child.copy(initialState = ByteString(initial)), payload = ByteString(payload))
      val digest = original.fingerprint
      payload(0) = 42
      initial(0) = 42
      original.payload shouldBe ByteString(0, 1, -1)
      original.child.initialState shouldBe ByteString(1, 0, -1)
      original.fingerprint shouldBe digest
      Vector(original.copy(deliveryId = "other"), original.copy(sourceRunId = "other"), original.copy(target = "other"),
        original.copy(payload = ByteString(1)), original.copy(child = original.child.copy(runId = "other")),
        original.copy(child = original.child.copy(definitionId = "other")),
        original.copy(child = original.child.copy(definitionVersion = "2")),
        original.copy(child = original.child.copy(codecVersion = "2")),
        original.copy(child = original.child.copy(initialState = ByteString(2)))).foreach(_.fingerprint should not be digest)
      original.copy(sourceRunId = "ab", target = "c").fingerprint should not be
        original.copy(sourceRunId = "a", target = "bc").fingerprint
    }

    "roundtrip every event and durable frontier with the pinned policy and only terminal result bytes" in {
      import DeliveryEvent._
      val completed = ChildTerminalRecorded(identity, Phase.Completed, result, None)
      val failed = ChildTerminalRecorded(identity, Phase.Failed, result, Some("child business failure"))
      val events: Vector[DeliveryEvent] = Vector(Accepted(request, request.fingerprint, policy),
        ChildStartIntent(1), ChildStartIntent(2), ChildQueryIntent(1),
        ChildRunning(identity, Some(100L)), ChildRunning(identity, None),
        Blocked("unknown child outcome"), Reconciled(1, 200L), completed, failed,
        ChildTerminalRecorded(identity, Phase.Completed, ByteString.empty, None))
      events.foreach { event =>
        serialization.findSerializerFor(event).identifier shouldBe 77102150
        roundtrip(event) shouldBe event
      }
      val empty = DeliveryState()
      val accepted = DeliveryState.evolve(empty, Accepted(request, request.fingerprint, policy))
      val starting = DeliveryState.evolve(accepted, ChildStartIntent(1))
      val startRecovered = DeliveryState.evolve(starting, ChildStartIntent(2))
      val running = DeliveryState.evolve(starting, ChildRunning(identity, Some(100L)))
      val querying = DeliveryState.evolve(running, ChildQueryIntent(1))
      val blocked = DeliveryState.evolve(querying, Blocked("unknown"))
      val reconciled = DeliveryState.evolve(blocked, Reconciled(1, 200L))
      val manualQuery = DeliveryState.evolve(reconciled, ChildQueryIntent(2))
      val missingChild = DeliveryState.evolve(querying, ChildStartIntent(2))
      val succeeded = DeliveryState.evolve(querying, completed)
      val knownFailed = DeliveryState.evolve(querying, failed)
      Vector(empty, accepted, starting, startRecovered, running, querying, blocked, reconciled,
        manualQuery, missingChild, succeeded, knownFailed).foreach { state =>
        state.invalid shouldBe None
        serialization.findSerializerFor(state).identifier shouldBe 77102150
        roundtrip(state) shouldBe state
        if (!state.terminal) state.result shouldBe None
      }
      accepted.policy shouldBe Some(policy)
      accepted.policy.get.limits shouldBe limits
      succeeded.result shouldBe Some(result)
      DeliveryState.evolve(succeeded, completed) shouldBe succeeded
      val conflict = DeliveryState.evolve(succeeded, failed)
      conflict.phase shouldBe Phase.Completed
      conflict.result shouldBe Some(result)
      conflict.invalid should not be empty
      DeliveryState.recoveryProblem(conflict, request.deliveryId, limits) should not be empty
      DeliveryState.recoveryProblem(accepted.copy(version = 2), request.deliveryId, limits) should not be empty
      DeliveryState.recoveryProblem(accepted, "wrong-entity", limits) should not be empty
    }

    "roundtrip every public wire variant with usable ActorRefs and no serialization of local commands" in {
      val replies = testKit.createTestProbe[Reply]()
      val empty = View(request.deliveryId, None, None, Phase.Empty, None, None)
      val views = Vector(empty,
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Accepted, None, None),
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Starting, None, None),
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Running, None, None),
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Blocked, None, Some("unknown")),
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Blocked, None, Some("unknown"), reconciling = true),
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Completed, Some(result), None),
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Completed, Some(ByteString.empty), None),
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Failed, Some(result), Some("business failure")),
        View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.RecoveryBlocked, None, Some("version")),
        View(request.deliveryId, None, None, Phase.RecoveryBlocked, None, Some("owner unavailable")))
      val wire: Vector[Wire] = Vector(Deliver(request, replies.ref), Get(request.deliveryId, replies.ref),
        Reconcile(request.deliveryId, replies.ref), Rejected(request.deliveryId, "conflict")) ++ views.map(Current)
      wire.foreach { value =>
        serialization.findSerializerFor(value).identifier shouldBe 77102151
        roundtrip(value) shouldBe value
      }
      roundtrip(Deliver(request, replies.ref)).asInstanceOf[Deliver].replyTo ! Current(empty)
      replies.expectMessage(Current(empty))
      roundtrip(Get(request.deliveryId, replies.ref)).asInstanceOf[Get].replyTo ! Current(empty)
      replies.expectMessage(Current(empty))
      roundtrip(Reconcile(request.deliveryId, replies.ref)).asInstanceOf[Reconcile].replyTo ! Current(empty)
      replies.expectMessage(Current(empty))
      serialization.serialize(Drive(1L)).isFailure shouldBe true
      serialization.serialize(PollDue(1L, 1L, 1L)).isFailure shouldBe true
      serialization.serialize(CallTimedOut(1L, 1L)).isFailure shouldBe true
    }

    "reject unsupported schemas, states, oneofs, enum values, nested unknowns and oversized input before replay" in {
      val serializer = serialization.findSerializerFor(DeliveryState()).asInstanceOf[DeliverySerializer]
      intercept[IllegalArgumentException](serializer.fromBinary(DeliveryEventPO(schemaVersion = 2).toByteArray, "DE1"))
      intercept[IllegalArgumentException](serializer.fromBinary(DeliveryEventPO(schemaVersion = 1).toByteArray, "DE1"))
      intercept[IllegalArgumentException](serializer.fromBinary(Array[Byte](8, 1, -94, 6, 0), "DE1"))
      val validState = DeliveryProtoConverters.stateToProto(DeliveryState())
      intercept[IllegalArgumentException](serializer.fromBinary(validState.copy(schemaVersion = 2).toByteArray, "DS1"))
      intercept[IllegalArgumentException](serializer.fromBinary(validState.copy(stateVersion = 2).toByteArray, "DS1"))
      intercept[IllegalArgumentException](serializer.fromBinary(
        validState.copy(phase = DeliveryPhasePO.Unrecognized(99)).toByteArray, "DS1"))
      intercept[IllegalArgumentException](serializer.fromBinary(
        validState.copy(nextAction = DeliveryActionPO.Unrecognized(99)).toByteArray, "DS1"))
      intercept[IllegalArgumentException](serializer.fromBinary(new Array[Byte](limits.maxEnvelopeBytes + 1), "DE1"))
      intercept[IllegalArgumentException](serializer.fromBinary(Array.emptyByteArray, "future-manifest"))
      val accepted = DeliveryProtoConverters.eventToProto(DeliveryEvent.Accepted(request, request.fingerprint, policy))
      val acceptedBody = accepted.event.asInstanceOf[DeliveryEventPO.Event.Accepted].value
      val requestProto = acceptedBody.request.get
      val unknownChild = DeliverySubmissionPO.parseFrom(requestProto.child.get.toByteArray ++ Array[Byte](-94, 6, 0))
      val nestedUnknown = accepted.copy(event = DeliveryEventPO.Event.Accepted(acceptedBody.copy(
        request = Some(requestProto.copy(child = Some(unknownChild))))))
      intercept[IllegalArgumentException](serializer.fromBinary(nestedUnknown.toByteArray, "DE1"))

      val replies = testKit.createTestProbe[Reply]()
      val wireSerializer = serialization.findSerializerFor(Get("delivery", replies.ref)).asInstanceOf[DeliveryWireSerializer]
      intercept[IllegalArgumentException](wireSerializer.fromBinary(DeliveryWirePO(schemaVersion = 1).toByteArray, "DW1"))
      intercept[IllegalArgumentException](wireSerializer.fromBinary(DeliveryWirePO(schemaVersion = 2).toByteArray, "DW1"))
      intercept[IllegalArgumentException](wireSerializer.fromBinary(Array[Byte](8, 1, -94, 6, 0), "DW1"))
      intercept[IllegalArgumentException](wireSerializer.fromBinary(new Array[Byte](limits.maxEnvelopeBytes + 1), "DW1"))
      val futureView = DeliveryViewPO(deliveryId = request.deliveryId, phase = DeliveryPhasePO.Unrecognized(99))
      intercept[IllegalArgumentException](wireSerializer.fromBinary(DeliveryWirePO(schemaVersion = 1,
        message = DeliveryWirePO.Message.Current(DeliveryCurrentPO(view = Some(futureView)))).toByteArray, "DW1"))
    }

    "refuse forged running/terminal snapshots, changed identities and counters beyond the frozen policy" in {
      import DeliveryEvent._
      val accepted = DeliveryState.evolve(DeliveryState(), Accepted(request, request.fingerprint, policy))
      val starting = DeliveryState.evolve(accepted, ChildStartIntent(1))
      val running = DeliveryState.evolve(starting, ChildRunning(identity, Some(100L)))
      val querying = DeliveryState.evolve(running, ChildQueryIntent(1))
      val terminal = DeliveryState.evolve(querying, ChildTerminalRecorded(identity, Phase.Completed, result, None))
      val invalid = Vector(accepted.copy(policy = None), accepted.copy(version = 2),
        accepted.copy(requestDigest = Some("0" * 64)),
        starting.copy(phase = Phase.Running, nextAction = Some(DeliveryState.Query), nextPollAt = Some(100L)),
        running.copy(childIdentity = Some(identity.copy(codecVersion = "2"))),
        running.copy(childIdentity = Some(identity.copy(initialDigest = "0" * 64))),
        running.copy(result = Some(result)), terminal.copy(result = None), terminal.copy(nextAction = Some(DeliveryState.Query)),
        running.copy(nextPollAt = Some(-1L)), running.copy(starts = policy.maxStarts + 1),
        running.copy(polls = -1), running.copy(remainingPolls = policy.maxPolls + 1),
        running.copy(reconciliations = policy.maxReconciliations + 1))
      invalid.foreach { state =>
        DeliveryValidation.state(state, limits) should not be empty
        serialization.serialize(state).isFailure shouldBe true
      }
      val wrongTerminal = DeliveryState.evolve(querying,
        ChildTerminalRecorded(identity.copy(planDigest = "0" * 64), Phase.Completed, result, None))
      wrongTerminal.invalid should not be empty
      wrongTerminal.result shouldBe None
      wrongTerminal.childIdentity shouldBe Some(identity)
      DeliveryState.evolve(accepted, ChildTerminalRecorded(identity, Phase.Completed, result, None)).invalid should not be empty
      DeliveryState.evolve(accepted, ChildQueryIntent(1)).invalid should not be empty
      DeliveryState.evolve(starting, ChildStartIntent(3)).invalid should not be empty
    }

    "enforce request, initial state, terminal, diagnostic and aggregate limits" in {
      val replies = testKit.createTestProbe[Reply]()
      Vector(request.copy(payload = ByteString(new Array[Byte](limits.maxRequestBytes + 1))),
        request.copy(child = child.copy(initialState = ByteString(new Array[Byte](limits.maxStateBytes + 1)))),
        request.copy(sourceRunId = "界" * (limits.maxStringBytes / 3 + 1)), request.copy(target = ""),
        request.copy(child = null)).foreach { invalid =>
        serialization.serialize(Deliver(invalid, replies.ref)).isFailure shouldBe true
      }
      serialization.serialize(Deliver(request, null)).isFailure shouldBe true
      serialization.serialize(DeliveryEvent.Accepted(request, "0" * 64, policy)).isFailure shouldBe true
      serialization.serialize(DeliveryEvent.ChildQueryIntent(-1)).isFailure shouldBe true
      serialization.serialize(DeliveryEvent.ChildTerminalRecorded(identity, Phase.Completed,
        ByteString(new Array[Byte](limits.maxResultBytes + 1)), None)).isFailure shouldBe true
      serialization.serialize(DeliveryEvent.ChildTerminalRecorded(identity, Phase.Running, result, None)).isFailure shouldBe true
      serialization.serialize(DeliveryEvent.ChildRunning(identity.copy(planDigest = "not-a-digest"), Some(1L))).isFailure shouldBe true
      serialization.serialize(Current(View(request.deliveryId, Some(request.fingerprint), Some(child.runId),
        Phase.Running, Some(result), None))).isFailure shouldBe true
      serialization.serialize(Current(View(request.deliveryId, Some(request.fingerprint), Some(child.runId),
        Phase.Completed, None, None))).isFailure shouldBe true
      serialization.serialize(Current(View(request.deliveryId, Some(request.fingerprint), Some(child.runId),
        Phase.Completed, Some(result), None, reconciling = true))).isFailure shouldBe true
      val diagnostic = limits.diagnostic("界" * limits.maxStringBytes)
      diagnostic.getBytes("UTF-8").length should be <= limits.maxStringBytes
      serialization.serialize(Rejected(request.deliveryId, diagnostic)).isSuccess shouldBe true
      val narrow = ProtocolLimits(maxRequestBytes = 16, maxStateBytes = 32, maxResultBytes = 32,
        maxEvidenceBytes = 16, maxEnvelopeBytes = 180, maxStringBytes = 64)
      DeliveryValidation.request(request, narrow).get should include("aggregate envelope")
      intercept[IllegalArgumentException](DeliverySettings(limits = narrow))
    }

    "validate durations, finite lifetime budgets and default configuration" in {
      val defaults = ConfigFactory.parseResources("delivery-reference.conf")
      DeliverySettings.fromConfig(defaults) shouldBe DeliverySettings()
      Vector("query-timeout = 0ms", "query-timeout = 8d", "poll-interval = 0ms", "poll-interval = 8d",
        "max-polls = 0", "max-polls = 2147483647", "limits.max-string-bytes = 63",
        "limits.max-envelope-bytes = 1", "limits.max-result-bytes = 0").foreach { field =>
        intercept[IllegalArgumentException](DeliverySettings.fromConfig(
          ConfigFactory.parseString(s"monarch.delivery.$field").withFallback(defaults)))
      }
      intercept[IllegalArgumentException](DeliverySettings(queryTimeout = 500.microseconds))
      intercept[IllegalArgumentException](DeliverySettings(maxPolls = Int.MaxValue))
    }
  }

  "DeliveryClient" should {
    "return the finite acceptance ask without waiting for business completion and expose get/reconcile" in {
      val requests = testKit.createTestProbe[Command]()
      val answers = testKit.createTestProbe[Try[Reply]]()
      val ref = testKit.spawn(Behaviors.receiveMessage[Command] { message => requests.ref ! message; Behaviors.same })
      val client = new DeliveryClient(_ => ref, 3.seconds)(ec, system.scheduler)
      val accepted = Current(View(request.deliveryId, Some(request.fingerprint), Some(child.runId), Phase.Accepted, None, None))
      try {
        client.deliver(request).onComplete(answer => answers.ref ! answer)
        val deliver = requests.receiveMessage().asInstanceOf[Deliver]
        deliver.request shouldBe request
        deliver.replyTo ! accepted
        answers.receiveMessage().get shouldBe accepted
        client.get(request.deliveryId).onComplete(answer => answers.ref ! answer)
        requests.receiveMessage().asInstanceOf[Get].replyTo ! accepted
        answers.receiveMessage().get shouldBe accepted
        client.reconcile(request.deliveryId).onComplete(answer => answers.ref ! answer)
        requests.receiveMessage().asInstanceOf[Reconcile].replyTo ! accepted
        answers.receiveMessage().get shouldBe accepted
      } finally testKit.stop(ref)
    }

    "fail an unanswered finite ask or failed resolver without spawning retries or long-lived waiters" in {
      val requests = testKit.createTestProbe[Command]()
      val answers = testKit.createTestProbe[Try[Reply]]()
      val ref = testKit.spawn(Behaviors.receiveMessage[Command] { message => requests.ref ! message; Behaviors.same })
      try {
        val client = new DeliveryClient(_ => ref, 100.millis)(ec, system.scheduler)
        client.deliver(request).onComplete(answer => answers.ref ! answer)
        requests.receiveMessage() shouldBe a[Deliver]
        answers.receiveMessage().isFailure shouldBe true
        requests.expectNoMessage(150.millis)
        val broken = new DeliveryClient(_ => throw new IllegalStateException("no owner"))(ec, system.scheduler)
        broken.get(request.deliveryId).onComplete(answer => answers.ref ! answer)
        answers.receiveMessage().isFailure shouldBe true
        client.deliver(null).onComplete(answer => answers.ref ! answer)
        answers.receiveMessage().isFailure shouldBe true
        intercept[IllegalArgumentException](new DeliveryClient(_ => ref, Duration.Zero)(ec, system.scheduler))
      } finally testKit.stop(ref)
    }
  }
}
