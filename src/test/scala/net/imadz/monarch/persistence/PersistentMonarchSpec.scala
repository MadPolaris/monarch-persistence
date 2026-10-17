package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.concurrent.duration._

/** Crash-recovery contract, exercised for real:
  *   1. run stages a, b, c — c blocks on a gate;
  *   2. after a and b journal their StageDone, stop the actor (crash);
  *   3. spawn again with the same persistenceId: recovery must resume from c
  *      WITHOUT re-running a or b;
  *   4. opening the gate lets the recovered run finish; each stage ran exactly once.
  */
class PersistentMonarchSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory
        .parseString("""
          |akka.persistence.journal.plugin = "akka.persistence.testkit.journal"
          |akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
          |akka.persistence.testkit.events.serialize = off
          |akka.actor.testkit.typed.single-expect-default = 10s
          |""".stripMargin)
        .withFallback(EventSourcedBehaviorTestKit.config))
    with AnyWordSpecLike
    with Matchers
    with Eventually {

  implicit override val patience: PatienceConfig = PatienceConfig(15.seconds, 100.millis)

  private implicit val ec: ExecutionContextExecutor = system.executionContext

  private def countOf(m: ConcurrentHashMap[String, AtomicInteger], k: String): Int =
    Option(m.get(k)).map(_.get()).getOrElse(0)

  "PersistentMonarch" should {
    "resume from the journal after a crash without re-running completed stages" in {
      val started = new ConcurrentHashMap[String, AtomicInteger]()
      def count(stage: String): Int =
        started.computeIfAbsent(stage, _ => new AtomicInteger()).incrementAndGet()

      val gateC = Promise[Unit]()
      val donePromise = Promise[String]()
      val observer = new RunObserver[String] {
        override def onCompleted(runId: String): Unit = { donePromise.trySuccess(runId); () }
      }

      def behavior() =
        PersistentMonarch.behavior[String, String](
          persistenceId = PersistenceId("monarch-test", "run-1"),
          stages = List("a", "b", "c"),
          interpreter = {
            case ("a", s) => count("a"); Future.successful(s + "a")
            case ("b", s) => count("b"); Future.successful(s + "b")
            case ("c", s) => count("c"); gateC.future.map(_ => s + "c")
            case (other, _) => Future.failed(new IllegalArgumentException(s"unknown stage $other"))
          },
          stateCodec = StateCodec.stringCodec,
          observer = observer
        )

      val ref1 = testKit.spawn(behavior(), "run-1")
      val probe = testKit.createTestProbe[PersistentMonarch.RunReply]()
      ref1 ! PersistentMonarch.StartRun("run-1", "", probe.ref)
      probe.expectMessage(PersistentMonarch.Accepted("run-1"))

      // a and b complete and journal; c starts and blocks on the gate.
      eventually { countOf(started, "a") shouldBe 1 }
      eventually { countOf(started, "b") shouldBe 1 }
      eventually { countOf(started, "c") shouldBe 1 }

      // ---- crash ----
      testKit.stop(ref1)

      // ---- restart: same persistenceId replays the journal, resumes from c ----
      testKit.spawn(behavior(), "run-1")
      eventually { countOf(started, "c") shouldBe 2 }
      Thread.sleep(500) // give any stray re-execution a chance to show up
      countOf(started, "a") shouldBe 1
      countOf(started, "b") shouldBe 1

      // Open the gate: the recovered run drains c and completes.
      gateC.success(())
      eventually { donePromise.isCompleted shouldBe true }
    }

    "reject a second StartRun for the same entity" in {
      val donePromise = Promise[String]()
      val observer = new RunObserver[String] {
        override def onCompleted(runId: String): Unit = { donePromise.trySuccess(runId); () }
      }
      val ref = testKit.spawn(
        PersistentMonarch.behavior[String, String](
          persistenceId = PersistenceId("monarch-test", "run-2"),
          stages = List("a"),
          interpreter = (st, s) => Future.successful(s + st),
          stateCodec = StateCodec.stringCodec,
          observer = observer
        ),
        "run-2"
      )
      val probe = testKit.createTestProbe[PersistentMonarch.RunReply]()
      ref ! PersistentMonarch.StartRun("run-2", "", probe.ref)
      probe.expectMessage(PersistentMonarch.Accepted("run-2"))
      eventually { donePromise.isCompleted shouldBe true }

      ref ! PersistentMonarch.StartRun("run-2", "", probe.ref)
      probe.expectMessageType[PersistentMonarch.Rejected]
    }
  }
}
