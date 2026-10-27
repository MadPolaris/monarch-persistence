package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.typed.PersistenceId
import akka.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import PersistentMonarch._

class PersistenceCompatibilitySpec extends ScalaTestWithActorTestKit(
    SerializedValidationJournal.config.withFallback(ConfigFactory.load()))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext

  "Persistence compatibility" should {
    "recover old Jackson records and append new Protobuf checkpoints" in {
      val id = PersistenceId.ofUniqueId("mixed-legacy-journal")
      val serialization = SerializationExtension(system.classicSystem)
      val jackson = serialization.serializerByIdentity.values.find(_.getClass.getName.endsWith("JacksonCborSerializer")).get
      val bytes = StateCodec.stringCodec.encode _
      val legacy: Vector[Event] = Vector(RunStarted(id.id, Vector("a#0", "b#1"), bytes("")),
        StageDone("a#0", 1L, Map.empty, bytes("a")))
      val encoded = legacy.zipWithIndex.map { case (event, index) =>
        SerializedValidationJournal.Stored(index + 1L, jackson.identifier, Serializers.manifestFor(jackson, event), jackson.toBinary(event))
      }
      SerializedValidationJournal.storage.put(id.id, encoded)
      val stages = testKit.createTestProbe[String]()
      val done = testKit.createTestProbe[String]()
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a", "b", "c"),
        (stage, state) => { stages.ref ! (stage + ":" + state); Future.successful(state + stage) }, StateCodec.stringCodec,
        observer = new RunObserver[String] { override def onCompleted(id: String): Unit = done.ref ! id }))
      stages.expectMessage("b:a")
      stages.expectMessage("c:ab")
      done.expectMessage(id.id)
      val records = SerializedValidationJournal.storage(id.id)
      records.take(2).map(_.serializerId).distinct shouldBe Vector(jackson.identifier)
      records.drop(2).map(_.serializerId).distinct shouldBe Vector(new MonarchSerializer().identifier)
      val events = records.map(r => serialization.deserialize(r.bytes, r.serializerId, r.manifest).get)
      events.collect { case event: RunPlanExtended => event.stageCursors } shouldBe Vector(Vector("a#0", "b#1", "c#2"))
      events.last shouldBe RunCompleted(id.id)
      testKit.stop(ref)
      SerializedValidationJournal.clear(id.id)
    }

    "stop on checkpoint encoding failure without invoking regrouping policy" in {
      val id = PersistenceId.ofUniqueId("codec-checkpoint-failure")
      val calls = new AtomicInteger()
      val codec = StateCodec[String](state => {
        if (state == "post") throw new IllegalArgumentException("encoding failed")
        state.getBytes("UTF-8")
      }, bytes => new String(bytes, "UTF-8"))
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a", "b"),
        (_, _) => Future.successful("post"), codec,
        failurePolicy = Some((_, _, state) => { calls.incrementAndGet(); Future.successful(state) })))
      val replies = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "initial", replies.ref)
      replies.expectMessage(Accepted(id.id))
      replies.expectTerminated(ref)
      calls.get() shouldBe 0
      SerializedValidationJournal.storage(id.id).size shouldBe 1
      SerializedValidationJournal.clear(id.id)
    }

    "reject invalid initial encoding without persisting or accepting a run" in {
      val id = PersistenceId.ofUniqueId("codec-initial-failure")
      val calls = new AtomicInteger()
      val codec = StateCodec[String](_ => throw new IllegalArgumentException("initial encoding failed"), _ => "unused")
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a"),
        (_, state) => { calls.incrementAndGet(); Future.successful(state) }, codec))
      val replies = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "initial", replies.ref)
      replies.expectMessageType[Rejected].reason should include("encode failed")
      calls.get() shouldBe 0
      SerializedValidationJournal.storage.get(id.id) shouldBe None
      testKit.stop(ref)
      SerializedValidationJournal.clear(id.id)
    }

    "not decode an unsupported or corrupt recovered state as a business failure" in {
      val id = PersistenceId.ofUniqueId("codec-recovery-failure")
      val serialization = SerializationExtension(system.classicSystem)
      val event = RunStarted(id.id, Vector("a#0"), Array[Byte](1))
      val serializer = serialization.findSerializerFor(event)
      SerializedValidationJournal.storage.put(id.id, Vector(SerializedValidationJournal.Stored(
        1L, serializer.identifier, Serializers.manifestFor(serializer, event), serializer.toBinary(event))))
      val calls = new AtomicInteger()
      val codec = StateCodec[String](_.getBytes("UTF-8"), _ => throw new IllegalArgumentException("codec version unsupported"))
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a"),
        (_, state) => { calls.incrementAndGet(); Future.successful(state) }, codec,
        failurePolicy = Some((_, _, state) => { calls.incrementAndGet(); Future.successful(state) })))
      testKit.createTestProbe[Unit]().expectTerminated(ref)
      calls.get() shouldBe 0
      SerializedValidationJournal.storage(id.id).size shouldBe 1
      SerializedValidationJournal.clear(id.id)
    }
  }
}
