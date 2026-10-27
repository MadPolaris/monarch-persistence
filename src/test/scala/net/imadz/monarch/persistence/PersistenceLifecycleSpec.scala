package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.typed.PersistenceId
import akka.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import PersistentMonarch._

class PersistenceLifecycleSpec extends ScalaTestWithActorTestKit(
    SerializedValidationJournal.config.withFallback(ConfigFactory.load()))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext

  "Ownership through persistence lifecycle" should {
    "not start an engine after stop during initial persistence" in {
      val id = PersistenceId.ofUniqueId("stop-initial-persist")
      val writes = testKit.createTestProbe[SerializedValidationJournal.Pending]()
      val stages = testKit.createTestProbe[String]()
      SerializedValidationJournal.gates.put(id.id, writes.ref)
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a"),
        (stage, state) => { stages.ref ! stage; Future.successful(state) }, StateCodec.stringCodec))
      val reply = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "", reply.ref)
      val start = writes.receiveMessage()
      testKit.stop(ref)
      start.release()
      stages.expectNoMessage(100.millis)
      reply.expectNoMessage(100.millis)
      SerializedValidationJournal.storage(id.id).size shouldBe 1
      SerializedValidationJournal.clear(id.id)
    }

    "revoke the owner when stopped with an unacknowledged checkpoint" in {
      val id = PersistenceId.ofUniqueId("stop-checkpoint-persist")
      val writes = testKit.createTestProbe[SerializedValidationJournal.Pending]()
      val stages = testKit.createTestProbe[String]()
      val notices = testKit.createTestProbe[String]()
      SerializedValidationJournal.gates.put(id.id, writes.ref)
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a", "b"),
        (stage, state) => { stages.ref ! stage; Future.successful(state + stage) }, StateCodec.stringCodec,
        observer = new DurableRunObserver[String] {
          override def onCheckpointCommitted(cursor: String, metadata: Map[String, String], state: String): Unit = notices.ref ! cursor
        }))
      val reply = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "", reply.ref)
      writes.receiveMessage().release()
      reply.expectMessage(Accepted(id.id))
      stages.expectMessage("a")
      val checkpoint = writes.receiveMessage()
      checkpoint.events.head.payload shouldBe a[StageDone]
      testKit.stop(ref)
      checkpoint.release()
      stages.expectNoMessage(100.millis)
      notices.expectNoMessage(100.millis)
      SerializedValidationJournal.storage(id.id).size shouldBe 2
      SerializedValidationJournal.clear(id.id)
    }

    "not acquire ownership or start stages after stop during journal replay" in {
      val id = PersistenceId.ofUniqueId("stop-journal-replay")
      val serializer = SerializationExtension(system.classicSystem)
      val event = RunStarted(id.id, Vector("a#0"), Array.emptyByteArray)
      val codec = serializer.findSerializerFor(event)
      SerializedValidationJournal.storage.put(id.id, Vector(SerializedValidationJournal.Stored(
        1L, codec.identifier, Serializers.manifestFor(codec, event), codec.toBinary(event))))
      val replay = testKit.createTestProbe[SerializedValidationJournal.Replay]()
      val stages = testKit.createTestProbe[String]()
      SerializedValidationJournal.replayProbes.put(id.id, replay.ref)
      SerializedValidationJournal.gatedReplay.put(id.id, true)
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a"),
        (stage, state) => { stages.ref ! stage; Future.successful(state) }, StateCodec.stringCodec))
      val pending = replay.receiveMessage()
      testKit.stop(ref)
      pending.release()
      stages.expectNoMessage(100.millis)
      SerializedValidationJournal.storage(id.id).size shouldBe 1
      SerializedValidationJournal.clear(id.id)
    }

    "not invoke business policy on an uncertain failed journal write" in {
      val id = PersistenceId.ofUniqueId("failed-journal-write")
      val writes = testKit.createTestProbe[SerializedValidationJournal.Pending]()
      val stages = testKit.createTestProbe[String]()
      val policy = testKit.createTestProbe[String]()
      SerializedValidationJournal.gates.put(id.id, writes.ref)
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a", "b"),
        (stage, state) => { stages.ref ! stage; Future.successful(state + stage) }, StateCodec.stringCodec,
        failurePolicy = Some((cursor, _, state) => { policy.ref ! cursor; Future.successful(state) })))
      val reply = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "", reply.ref)
      writes.receiveMessage().release()
      reply.expectMessage(Accepted(id.id))
      stages.expectMessage("a")
      writes.receiveMessage().fail(new IllegalStateException("journal write failed"))
      reply.expectTerminated(ref)
      stages.expectNoMessage(100.millis)
      policy.expectNoMessage(100.millis)
      SerializedValidationJournal.storage(id.id).size shouldBe 1
      SerializedValidationJournal.clear(id.id)
    }
  }
}
