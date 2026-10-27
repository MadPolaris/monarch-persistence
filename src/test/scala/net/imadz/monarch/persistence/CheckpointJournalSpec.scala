package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{BehaviorInterceptor, SupervisorStrategy, TypedActorContext}
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import PersistentMonarch._

class CheckpointJournalSpec extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
  akka.persistence.journal.plugin = "controlled-journal"
  controlled-journal.class = "net.imadz.monarch.persistence.ControlledJournal"
  controlled-journal.plugin-dispatcher = "akka.actor.default-dispatcher"
  akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
  akka.actor.testkit.typed.single-expect-default = 5s
""")) with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext

  "Journal acknowledgement" should {
    "hold the next stage and final success until their durable boundaries commit" in {
      val id = PersistenceId.ofUniqueId("gated-ack")
      val writes = testKit.createTestProbe[ControlledJournal.Pending]()
      val stages = testKit.createTestProbe[String]()
      val done = testKit.createTestProbe[String]()
      ControlledJournal.probes.put(id.id, writes.ref)
      val ref = testKit.spawn(PersistentMonarch.behavior[String, String](id, List("a", "b"),
        (stage, state) => { stages.ref ! stage; Future.successful(state + stage) }, StateCodec.stringCodec,
        observer = new RunObserver[String] { override def onCompleted(id: String): Unit = done.ref ! id }))
      val reply = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "", reply.ref)
      val start = writes.receiveMessage()
      start.events.head.payload shouldBe a[RunStarted]
      stages.expectNoMessage(100.millis)
      start.release()
      reply.expectMessage(Accepted(id.id))
      stages.expectMessage("a")
      val first = writes.receiveMessage()
      first.events.head.payload.asInstanceOf[StageDone].cursor shouldBe "a#0"
      stages.expectNoMessage(100.millis)
      done.expectNoMessage(100.millis)
      first.release()
      stages.expectMessage("b")
      val second = writes.receiveMessage()
      second.events.head.payload.asInstanceOf[StageDone].cursor shouldBe "b#1"
      done.expectNoMessage(100.millis)
      second.release()
      val terminal = writes.receiveMessage()
      terminal.events.head.payload shouldBe RunCompleted(id.id)
      done.expectNoMessage(100.millis)
      terminal.release()
      done.expectMessage(id.id)
      testKit.stop(ref)
      ControlledJournal.probes.remove(id.id)
    }

    "stop advancing on timeout without invoking business policy or writing a failure terminal" in {
      val id = PersistenceId.ofUniqueId("gated-timeout")
      val writes = testKit.createTestProbe[ControlledJournal.Pending]()
      val policy = new AtomicInteger()
      val stages = testKit.createTestProbe[String]()
      ControlledJournal.probes.put(id.id, writes.ref)
      val ref = testKit.spawn(PersistentMonarch.behaviorWithSettings[String, String](id, List("a", "b"),
        (stage, state) => { stages.ref ! stage; Future.successful(state + stage) }, StateCodec.stringCodec,
        PersistentMonarchSettings(checkpointTimeout = 300.millis),
        failurePolicy = Some((_, _, state) => { policy.incrementAndGet(); Future.successful(state) })))
      val reply = testKit.createTestProbe[RunReply]()
      val stopped = testKit.createTestProbe[Unit]()
      ref ! StartRun(id.id, "", reply.ref)
      writes.receiveMessage().release()
      reply.expectMessage(Accepted(id.id))
      stages.expectMessage("a")
      val pending = writes.receiveMessage()
      stages.expectNoMessage(600.millis)
      // Release a late commit: it must not make the old engine continue.
      pending.release()
      stopped.expectTerminated(ref)
      stages.expectNoMessage(100.millis)
      policy.get() shouldBe 0
      ControlledJournal.storage(id.id).map(_.payload).exists(_.isInstanceOf[RunFailedEvt]) shouldBe false
      ControlledJournal.probes.remove(id.id)
    }

    "ignore old callbacks retained by a same-ref supervised restart" in {
      val id = PersistenceId.ofUniqueId("same-ref-restart")
      val calls = new AtomicInteger()
      val gate = Promise[String]()
      val started = testKit.createTestProbe[Int]()
      val done = testKit.createTestProbe[String]()
      def underlying = PersistentMonarch.behavior[String, String](id, List("a"), (_, state) => {
        val count = calls.incrementAndGet()
        started.ref ! count
        if (count == 1) gate.future else Future.successful(state + "new")
      }, StateCodec.stringCodec, observer = new RunObserver[String] { override def onCompleted(id: String): Unit = done.ref ! id })
      val intercepted = Behaviors.intercept(() => new BehaviorInterceptor[Command[String], Command[String]] {
        override def aroundReceive(ctx: TypedActorContext[Command[String]], message: Command[String],
            target: BehaviorInterceptor.ReceiveTarget[Command[String]]) = message match {
          case RunFailed("test", "restart") => throw new IllegalStateException("restart test")
          case _ => target(ctx, message)
        }
      })(underlying)
      val ref = testKit.spawn(Behaviors.supervise(intercepted).onFailure[IllegalStateException](SupervisorStrategy.restart))
      val reply = testKit.createTestProbe[RunReply]()
      ref ! StartRun(id.id, "", reply.ref)
      reply.expectMessage(Accepted(id.id))
      started.expectMessage(1)
      // Ordinary mailbox retains these messages across restart. Their identities
      // must be checked by the new behavior, not only before sending.
      ref ! RunFailed[String]("test", "restart")
      ref ! Checkpoint[String](-1L, 0, "a#0", Map.empty, "old", None, Promise[Unit]())
      ref ! Finished[String](-1L, None)
      started.expectMessage(2)
      gate.success("old")
      done.expectMessage(id.id)
      ControlledJournal.storage(id.id).map(_.payload).collect { case event: StageDone => event.snapshot.toVector } shouldBe
        Vector(StateCodec.stringCodec.encode("new").toVector)
      testKit.stop(ref)
    }
  }
}
