package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import scala.concurrent.{Future, Promise}
import PersistentMonarch._

class SnapshotRecoverySpec extends ScalaTestWithActorTestKit(
    SerializedValidationJournal.config.withFallback(ConfigFactory.load()))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext

  "Durable snapshots" should {
    "recover a complete snapshot plus journal tail without replaying the prefix" in {
      val id = PersistenceId.ofUniqueId("snapshot-tail")
      val snapshots = testKit.createTestProbe[ValidationSnapshotStore.Notice]()
      val replays = testKit.createTestProbe[SerializedValidationJournal.Replay]()
      val entries = testKit.createTestProbe[(String, String)]()
      val done = testKit.createTestProbe[String]()
      val gate = Promise[String]()
      ValidationSnapshotStore.probes.put(id.id, snapshots.ref)
      SerializedValidationJournal.replayProbes.put(id.id, replays.ref)
      def behavior() = PersistentMonarch.behaviorWithSettings[String, String](id, List("a", "b", "tail"),
        (stage, state) => {
          entries.ref ! (stage -> state)
          if (stage == "tail") gate.future.map(state + _) else Future.successful(state + stage)
        }, StateCodec.stringCodec, PersistentMonarchSettings(snapshotEvery = Some(2)),
        observer = new RunObserver[String] { override def onCompleted(id: String): Unit = done.ref ! id })
      val first = testKit.spawn(behavior())
      val reply = testKit.createTestProbe[RunReply]()
      snapshots.receiveMessage().kind shouldBe "load"
      first ! StartRun(id.id, "", reply.ref)
      reply.expectMessage(Accepted(id.id))
      entries.expectMessage("a" -> "")
      entries.expectMessage("b" -> "a")
      entries.expectMessage("tail" -> "ab")
      val saved = snapshots.receiveMessage()
      saved.kind shouldBe "saved"
      saved.sequence shouldBe 2L
      saved.stored.get.serializerId shouldBe new MonarchSerializer().identifier
      testKit.stop(first)
      val second = testKit.spawn(behavior())
      val loaded = snapshots.receiveMessage()
      loaded.kind shouldBe "load"
      loaded.sequence shouldBe 2L
      val replayed = replays.receiveMessage()
      replayed.from shouldBe 3L
      replayed.sequences shouldBe Vector(3L)
      entries.expectMessage("tail" -> "ab")
      gate.success("tail")
      done.expectMessage(id.id)
      testKit.stop(second)
      SerializedValidationJournal.clear(id.id)
    }

    "fall back to full journal recovery after a failed snapshot save" in {
      val id = PersistenceId.ofUniqueId("snapshot-save-failure")
      val snapshots = testKit.createTestProbe[ValidationSnapshotStore.Notice]()
      val replays = testKit.createTestProbe[SerializedValidationJournal.Replay]()
      val entries = testKit.createTestProbe[String]()
      val gate = Promise[String]()
      ValidationSnapshotStore.probes.put(id.id, snapshots.ref)
      ValidationSnapshotStore.failSaves.put(id.id, true)
      SerializedValidationJournal.replayProbes.put(id.id, replays.ref)
      def behavior() = PersistentMonarch.behaviorWithSettings[String, String](id, List("a", "tail"),
        (stage, state) => {
          entries.ref ! (stage + ":" + state)
          if (stage == "tail") gate.future.map(state + _) else Future.successful(state + stage)
        }, StateCodec.stringCodec, PersistentMonarchSettings(snapshotEvery = Some(2)))
      val first = testKit.spawn(behavior())
      val reply = testKit.createTestProbe[RunReply]()
      snapshots.receiveMessage()
      first ! StartRun(id.id, "", reply.ref)
      reply.expectMessage(Accepted(id.id))
      entries.expectMessage("a:")
      snapshots.receiveMessage().kind shouldBe "save-failed"
      entries.expectMessage("tail:a")
      testKit.stop(first)
      val second = testKit.spawn(behavior())
      snapshots.receiveMessage().sequence shouldBe 0L
      val replayed = replays.receiveMessage()
      replayed.from shouldBe 1L
      replayed.sequences shouldBe Vector(1L, 2L)
      entries.expectMessage("tail:a")
      testKit.stop(second)
      SerializedValidationJournal.clear(id.id)
    }
  }
}
