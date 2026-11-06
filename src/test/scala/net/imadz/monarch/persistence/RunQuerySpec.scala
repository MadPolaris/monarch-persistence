package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.typed.PersistenceId
import akka.serialization.{SerializationExtension, Serializers}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.RunProtocol._
import net.imadz.monarch.persistence.proto.{DonePO, DurableStatePO, RunStatePO, RunWirePO}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class RunQuerySpec extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
  akka.persistence.journal.plugin = "controlled-journal"
  controlled-journal.class = "net.imadz.monarch.persistence.ControlledJournal"
  controlled-journal.plugin-dispatcher = "akka.actor.default-dispatcher"
  akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
  akka.actor.allow-java-serialization = off
""")) with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
  private def id(): PersistenceId = PersistenceId.ofUniqueId("run-query-" + UUID.randomUUID())
  private val definition = RunDefinition("query", "1", "utf8-1", Vector("first", "last"))
  private val codec = StateCodec.stringCodec
  private def start(runId: String, reply: akka.actor.typed.ActorRef[Reply], bytes: String = ""): StartOrAttach =
    StartOrAttach(runId, definition.id, definition.version, definition.codecVersion, ByteString(bytes), reply)
  private def state(ref: akka.actor.typed.ActorRef[PersistentMonarch.Command[String]]): View = {
    val reply = createTestProbe[Reply]()
    ref ! GetRun(reply.ref)
    reply.receiveMessage().asInstanceOf[Current].view
  }

  "Run attachment and query" should {
    "acknowledge a start only after commit and retain the same result after terminal recovery" in {
      val pid = id()
      val writes = createTestProbe[ControlledJournal.Pending]()
      val stages = createTestProbe[String]()
      ControlledJournal.probes.put(pid.id, writes.ref)
      def behavior = PersistentMonarch.versionedBehavior(pid, definition,
        (step: String, s: String) => { stages.ref ! step; Future.successful(s + step) }, codec)
      var ref = spawn(behavior)
      val reply = createTestProbe[Reply]()
      ref ! start(pid.id, reply.ref)
      val first = writes.receiveMessage()
      first.events.head.payload shouldBe a[PersistentMonarch.VersionedRunStarted]
      reply.expectNoMessage(100.millis)
      stages.expectNoMessage(100.millis)
      first.release()
      reply.receiveMessage().asInstanceOf[Current].view.status shouldBe Status.Running
      stages.expectMessage("first")
      val checkpoint = writes.receiveMessage()
      stages.expectNoMessage(100.millis)
      checkpoint.release()
      stages.expectMessage("last")
      writes.receiveMessage().release()
      val terminal = writes.receiveMessage()
      terminal.events.head.payload shouldBe PersistentMonarch.RunCompleted(pid.id)
      terminal.release()
      createTestProbe[Unit]().awaitAssert(state(ref).status shouldBe Status.Completed)
      state(ref).lastState shouldBe Some(ByteString("firstlast"))
      testKit.stop(ref)
      ControlledJournal.probes.remove(pid.id)
      ref = spawn(behavior)
      ref ! start(pid.id, reply.ref)
      val attached = reply.receiveMessage().asInstanceOf[Current].view
      attached.status shouldBe Status.Completed
      attached.lastState shouldBe Some(ByteString("firstlast"))
      attached.completedCursors shouldBe definition.cursors
      stages.expectNoMessage(100.millis)
      testKit.stop(ref)
    }

    "attach without starting another engine and reject same id with different request or definition" in {
      val pid = id()
      val gate = Promise[String]()
      val calls = new AtomicInteger()
      val ref = spawn(PersistentMonarch.versionedBehavior(pid, definition,
        (_: String, _: String) => { calls.incrementAndGet(); gate.future }, codec))
      val reply = createTestProbe[Reply]()
      ref ! start(pid.id, reply.ref)
      reply.receiveMessage() shouldBe a[Current]
      createTestProbe[Unit]().awaitAssert(calls.get() shouldBe 1)
      (1 to 4).foreach { _ => ref ! start(pid.id, reply.ref); reply.receiveMessage() shouldBe a[Current] }
      ref ! start(pid.id, reply.ref, "different")
      reply.receiveMessage() shouldBe a[Rejected]
      ref ! start(pid.id, reply.ref).copy(definitionVersion = "2")
      reply.receiveMessage() shouldBe a[Rejected]
      calls.get() shouldBe 1
      testKit.stop(ref)
      gate.success("late")
    }

    "report a changed plan or undecodable checkpoint as RecoveryBlocked without running business code" in {
      val pid = id()
      val gate = Promise[String]()
      val calls = new AtomicInteger()
      val original = spawn(PersistentMonarch.versionedBehavior(pid, definition,
        (_: String, _: String) => { calls.incrementAndGet(); gate.future }, codec))
      val reply = createTestProbe[Reply]()
      original ! start(pid.id, reply.ref)
      reply.receiveMessage() shouldBe a[Current]
      createTestProbe[Unit]().awaitAssert(calls.get() shouldBe 1)
      testKit.stop(original)
      val changed = spawn(PersistentMonarch.versionedBehavior(pid, definition.copy(stages = Vector("changed")),
        (_: String, s: String) => { calls.incrementAndGet(); Future.successful(s) }, codec))
      state(changed).status shouldBe Status.RecoveryBlocked
      calls.get() shouldBe 1
      testKit.stop(changed)
      val broken = StateCodec[String](_.getBytes("UTF-8"), _ => throw new IllegalArgumentException("wrong codec"))
      val undecodable = spawn(PersistentMonarch.versionedBehavior(pid, definition,
        (_: String, s: String) => { calls.incrementAndGet(); Future.successful(s) }, broken))
      createTestProbe[Unit]().awaitAssert(state(undecodable).status shouldBe Status.RecoveryBlocked)
      calls.get() shouldBe 1
      testKit.stop(undecodable)
      gate.success("late")
    }

    "retain the initial bytes for an empty plan and distinguish failed checkpoints from external results" in {
      val pid = id()
      val empty = definition.copy(stages = Vector.empty[String])
      val ref = spawn(PersistentMonarch.versionedBehavior(pid, empty,
        (_: String, s: String) => Future.successful(s), codec))
      val reply = createTestProbe[Reply]()
      ref ! start(pid.id, reply.ref, "initial")
      reply.receiveMessage() shouldBe a[Current]
      createTestProbe[Unit]().awaitAssert(state(ref).status shouldBe Status.Completed)
      state(ref).lastState shouldBe Some(ByteString("initial"))
      testKit.stop(ref)
      val failedPid = id()
      val failed = spawn(PersistentMonarch.versionedBehavior(failedPid, definition,
        (step: String, s: String) => if (step == "first") Future.successful(s + "committed")
          else Future.failed(new IllegalArgumentException("business failure")), codec))
      failed ! start(failedPid.id, reply.ref)
      reply.receiveMessage() shouldBe a[Current]
      createTestProbe[Unit]().awaitAssert(state(failed).status shouldBe Status.Failed)
      state(failed).lastState shouldBe Some(ByteString("committed"))
      state(failed).completedCursors shouldBe Vector("first#0")
      testKit.stop(failed)
    }

    "return a serializable unavailable reply for an old committed state above the new wire limit" in {
      val pid = id()
      ControlledJournal.storage.put(pid.id, Vector(akka.persistence.PersistentRepr(
        PersistentMonarch.RunStarted(pid.id, Vector.empty, new Array[Byte](ProtocolLimits().maxStateBytes + 1)), 1L, pid.id)))
      val ref = spawn(PersistentMonarch.behavior(pid, Vector.empty[String],
        (_: String, s: String) => Future.successful(s), codec))
      val reply = createTestProbe[Reply]()
      ref ! GetRun(reply.ref)
      val unavailable = reply.receiveMessage().asInstanceOf[Unavailable]
      unavailable.reason should include("wire limits")
      SerializationExtension(system.classicSystem).serialize(unavailable).isSuccess shouldBe true
      testKit.stop(ref)
    }

    "bound codec rejection diagnostics so error replies remain serializable" in {
      val pid = id()
      val broken = StateCodec[String](_.getBytes("UTF-8"), _ => throw new IllegalArgumentException("坏消息🙂" * 3000))
      val ref = spawn(PersistentMonarch.versionedBehavior(pid, definition,
        (_: String, s: String) => Future.successful(s), broken))
      val reply = createTestProbe[Reply]()
      ref ! start(pid.id, reply.ref)
      val rejected = reply.receiveMessage().asInstanceOf[Rejected]
      rejected.reason.getBytes("UTF-8").length should be <= ProtocolLimits().maxStringBytes
      SerializationExtension(system.classicSystem).serialize(rejected).isSuccess shouldBe true
      state(ref).status shouldBe Status.NotStarted
      testKit.stop(ref)
    }

    "reject oversized starts without persisting and leave legacy StartRun behavior unchanged" in {
      val pid = id()
      val limits = ProtocolLimits(maxStateBytes = 2)
      val ref = spawn(PersistentMonarch.versionedBehavior(pid, definition,
        (_: String, s: String) => Future.successful(s), codec, limits = limits))
      val reply = createTestProbe[Reply]()
      ref ! start(pid.id, reply.ref, "too big")
      reply.receiveMessage() shouldBe a[Rejected]
      state(ref).status shouldBe Status.NotStarted
      ControlledJournal.storage.get(pid.id).toVector.flatten shouldBe empty
      testKit.stop(ref)
    }
  }

  "Run wire and legacy snapshots" should {
    "roundtrip concrete commands, replies and a usable reply actor with Java serialization disabled" in {
      val serialization = SerializationExtension(system.classicSystem)
      val reply = createTestProbe[Reply]()
      val request = start("wire-run", reply.ref)
      val view = View(Some("wire-run"), Status.AwaitingOperation, Vector.empty, Some(ByteString.empty),
        Some(definition.identity("wire-run", ByteString.empty)), Some(AwaitingRef("first#0", "operation", "digest")), None)
      val values: Vector[Wire] = Vector(request, GetRun(reply.ref), Current(view), Rejected("wire-run", "conflict"),
        Unavailable(Some("wire-run"), "historical result exceeds wire limits"))
      values.foreach { value =>
        val serializer = serialization.findSerializerFor(value)
        serializer shouldBe a[RunProtocolSerializer]
        val decoded = serialization.deserialize(serialization.serialize(value).get, serializer.identifier,
          Serializers.manifestFor(serializer, value)).get
        decoded shouldBe value
      }
      val decoded = serialization.deserialize(serialization.serialize(request).get, 77102123, "RW1").get.asInstanceOf[StartOrAttach]
      decoded.replyTo ! Current(view)
      reply.expectMessage(Current(view))
      intercept[IllegalArgumentException] {
        serialization.deserialize(RunWirePO(schemaVersion = 9).toByteArray, 77102123, "RW1").get
      }
    }

    "read an old S1 terminal with unavailable result and write extended snapshots as S2" in {
      val serializer = new MonarchSerializer()
      // Frozen S1 encoding: schema=1, run Done("old"), plan ["first#0"], state_version=1.
      val bytes = java.util.Base64.getDecoder.decode("CAESBxoFCgNvbGQaB2ZpcnN0IzAoAQ==")
      val old = serializer.fromBinary(bytes, "S1").asInstanceOf[PersistentMonarch.DurableState]
      old.run shouldBe PersistentMonarch.Done("old")
      old.retainedSnapshot shouldBe None
      old.identity shouldBe None
      serializer.manifest(old) shouldBe "S2"
      val decoded = serializer.fromBinary(serializer.toBinary(old), "S2").asInstanceOf[PersistentMonarch.DurableState]
      decoded shouldBe old
      val malformed = DurableStatePO(schemaVersion = 1, run = Some(RunStatePO(state = RunStatePO.State.Done(DonePO("old")))),
        stateVersion = 1, retainedSnapshot = Some(com.google.protobuf.ByteString.EMPTY))
      intercept[IllegalArgumentException](serializer.fromBinary(malformed.toByteArray, "S1"))
    }
  }
}
