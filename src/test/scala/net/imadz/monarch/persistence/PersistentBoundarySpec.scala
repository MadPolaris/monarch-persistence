package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.testkit.scaladsl.{EventSourcedBehaviorTestKit, PersistenceTestKit}
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.{StageError, StageFailedException}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import PersistentMonarch._

class PersistentBoundarySpec extends ScalaTestWithActorTestKit(
    ConfigFactory.parseString("""
      akka.persistence.journal.plugin = "akka.persistence.testkit.journal"
      akka.persistence.snapshot-store.plugin = "akka.persistence.testkit.snapshotstore.pluginid"
      akka.persistence.testkit.events.serialize = on
      akka.persistence.testkit.snapshots.serialize = on
      akka.actor.allow-java-serialization = off
      akka.actor.testkit.typed.single-expect-default = 5s
    """).withFallback(EventSourcedBehaviorTestKit.config))
    with AnyWordSpecLike with Matchers with Eventually {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
  implicit override val patience: PatienceConfig = PatienceConfig(10.seconds, 30.millis)
  private val journal = PersistenceTestKit(system)

  "Persistent boundaries" should {
    "commit resolved state and resume after it without rerunning classification" in {
      val id = PersistenceId.ofUniqueId("resolved-recovery")
      val classified = new AtomicInteger()
      val gate = Promise[String]()
      val waiting = testKit.createTestProbe[String]()
      val done = testKit.createTestProbe[String]()
      val durable = testKit.createTestProbe[String]()
      val observer = new DurableRunObserver[String] {
        override def onResolutionCommitted(cursor: String, error: StageError, snapshot: String): Unit = durable.ref ! snapshot
        override def onCompleted(runId: String): Unit = done.ref ! runId
      }
      def behavior() = PersistentMonarch.behavior[String, String](id, List("classify", "wait"), {
        case ("classify", _) => classified.incrementAndGet(); Future.failed(StageFailedException(StageError("classify#0", Some("REGROUP"), "REGROUP", "application split")))
        case (_, state) => waiting.ref ! state; gate.future.map(state + _)
      }, StateCodec.stringCodec, failurePolicy = Some((_, _, state) => Future.successful(state + "child-refs")), observer = observer)
      val first = testKit.spawn(behavior())
      val reply = testKit.createTestProbe[RunReply]()
      first ! StartRun("resolved-recovery", "", reply.ref)
      reply.expectMessage(Accepted("resolved-recovery"))
      durable.expectMessage("child-refs")
      waiting.expectMessage("child-refs")
      journal.persistedInStorage(id.id).collect { case event: StageResolved => event }.size shouldBe 1
      testKit.stop(first)
      val second = testKit.spawn(behavior())
      waiting.expectMessage("child-refs")
      classified.get() shouldBe 1
      gate.success("/done")
      done.expectMessage("resolved-recovery")
      classified.get() shouldBe 1
      testKit.stop(second)
    }

    "not let observer exceptions prevent progress or resolution" in {
      val id = PersistenceId.ofUniqueId("throwing-observer")
      val policy = new AtomicInteger()
      def fail(): Unit = throw new IllegalArgumentException("metrics failed")
      val observer = new DurableRunObserver[String] {
        override def onStart(id: String): Unit = fail()
        override def onStageFailed(cursor: String, detail: String): Unit = fail()
        override def onStageComplete(cursor: String, metadata: Map[String, String], snapshot: String): Unit = fail()
        override def onResolutionCommitted(cursor: String, error: StageError, snapshot: String): Unit = fail()
        override def onCheckpointCommitted(cursor: String, metadata: Map[String, String], snapshot: String): Unit = fail()
        override def onCompleted(id: String): Unit = fail()
      }
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("bad", "good"), {
        case ("bad", _) => Future.failed(StageFailedException(StageError("bad#0", None, "REGROUP", "split")))
        case (_, state) => Future.successful(state + "/done")
      }, StateCodec.stringCodec, failurePolicy = Some((_, _, state) => { policy.incrementAndGet(); Future.successful(state + "resolved") }), observer = observer))
      val reply = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "", reply.ref)
      reply.expectMessage(Accepted(id.id))
      eventually { journal.persistedInStorage(id.id).last shouldBe RunCompleted(id.id) }
      policy.get() shouldBe 1
      ref ! StartRun(id.id, "", reply.ref)
      reply.expectMessageType[Rejected]
      testKit.stop(ref)
    }

    "revoke ownership on stop even without a replacement" in {
      val id = PersistenceId.ofUniqueId("owner-stopped")
      val gate = Promise[String]()
      val entered = testKit.createTestProbe[String]()
      val next = new AtomicInteger()
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a", "b"), {
        case ("a", _) => entered.ref ! "a"; gate.future
        case (_, state) => next.incrementAndGet(); Future.successful(state)
      }, StateCodec.stringCodec))
      val reply = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "", reply.ref)
      reply.expectMessage(Accepted(id.id))
      entered.expectMessage("a")
      testKit.stop(ref)
      gate.success("a")
      // A direct execution callback signal would be observable if ownership survived.
      entered.expectNoMessage(200.millis)
      next.get() shouldBe 0
      journal.persistedInStorage(id.id).size shouldBe 1
    }

    "reject ambiguous legacy history before invoking the interpreter" in {
      val id = PersistenceId.ofUniqueId("legacy-hole")
      val bytes = StateCodec.stringCodec.encode _
      journal.persistForRecovery(id.id, List(RunStarted(id.id, Vector("a#0", "b#1", "c#2"), bytes("")),
        StageDone("a#0", 1L, Map.empty, bytes("a")), StageDone("c#2", 2L, Map.empty, bytes("aRc"))))
      val calls = new AtomicInteger()
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a", "b", "c"),
        (_, state) => { calls.incrementAndGet(); Future.successful(state) }, StateCodec.stringCodec))
      val reply = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "", reply.ref)
      reply.expectMessageType[Rejected]
      calls.get() shouldBe 0
      journal.persistedInStorage(id.id).size shouldBe 3
      testKit.stop(ref)
    }

    "serialize legacy events and the complete snapshot envelope without Java serialization" in {
      val serialization = SerializationExtension(system.classicSystem)
      val bytes = StateCodec.stringCodec.encode("state")
      val values: List[AnyRef] = List(RunStarted("id", Vector("a#0"), bytes),
        StageDone("a#0", 1L, Map("key" -> "value"), bytes),
        StageResolved("a#0", 2L, Some("REGROUP"), "REGROUP", "split", bytes),
        RunCompleted("id"), RunFailedEvt("a#0", "error"), RunPlanExtended(Vector("a#0")),
        DurableState(Executing("id", Vector("a#0"), bytes), Vector("a#0", "b#1"), None),
        DurableState(Idle, Vector.empty, None))
      values.foreach { value =>
        val serializer = serialization.findSerializerFor(value)
        serializer.getClass shouldBe classOf[MonarchSerializer]
        val encoded = serialization.serialize(value).get
        val decoded = serialization.deserialize(encoded, serializer.identifier,
          akka.serialization.Serializers.manifestFor(serializer, value)).get
        decoded.getClass shouldBe value.getClass
        decoded match {
          case state: DurableState =>
            state.plan shouldBe value.asInstanceOf[DurableState].plan
            state.run.getClass shouldBe value.asInstanceOf[DurableState].run.getClass
          case _ => ()
        }
      }
    }
  }
}
