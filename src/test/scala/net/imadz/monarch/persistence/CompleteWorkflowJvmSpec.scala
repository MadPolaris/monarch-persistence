package net.imadz.monarch.persistence

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Whole workflow faults: task effects, admission, required child completion and
  * drained input are checked separately against the independent business ledger.
  */
class CompleteWorkflowJvmSpec extends AnyWordSpec with Matchers {
  import RecoveryProcessFixture._
  private val externalMain = "net.imadz.monarch.persistence.ExternalTaskServiceFixture"
  private val hostMain = "net.imadz.monarch.persistence.CompleteWorkflowJvmHost"
  private def external(root: Path): (Child, Int) = {
    val dir = Files.createDirectory(root.resolve("external"))
    val service = start(externalMain, Vector(dir.toString), root, "external", getClass.getClassLoader)
    service -> service.expect("EXTERNAL_READY ").stripPrefix("EXTERNAL_READY ").trim.toInt
  }
  private def host(root: Path, port: Int, kind: String, mode: String, held: String = "", forced: Boolean = false,
      heldId: String = ""): Child = {
    val dir = root.resolve("host"); Files.createDirectories(dir)
    start(hostMain, Vector(dir.toString, port.toString, kind, "workflow-" + kind, mode, held, forced.toString, heldId),
      root, mode, getClass.getClassLoader)
  }
  private def call(port: Int, name: String, fields: String*): Vector[String] = ExternalTaskServiceFixture.call(port, name, fields.toVector)
  private def finishTasks(port: Int, child: Child): Unit = {
    val deadline = System.nanoTime() + 60.seconds.toNanos
    var done = false
    while (!done && System.nanoTime() < deadline && child.process.isAlive) {
      val tasks = call(port, "TASKS")
      tasks.head shouldBe "TASKS"
      tasks.tail.foreach { id => call(port, "COMPLETE", id).head shouldBe "SUCCEEDED" }
      done = child.transcript.asScala.exists(_.contains("WORKFLOW_DONE"))
      if (!done) Thread.sleep(80) // Polls an independent external system; no local scheduler timing assumption.
    }
    child.expect("WORKFLOW_DONE", 5.seconds)
    child.transcript.asScala.exists(_.startsWith("WORKFLOW_RESOURCES_CLOSED ")) shouldBe true
    child.finish()
  }
  private def reachGate(port: Int, child: Child, event: String, forced: Boolean): Unit = {
    val marker = s"HELD $event forced=$forced"
    val deadline = System.nanoTime() + 45.seconds.toNanos
    while (!child.transcript.asScala.exists(_.contains(marker)) && System.nanoTime() < deadline && child.process.isAlive) {
      val tasks = call(port, "TASKS")
      tasks.head shouldBe "TASKS"
      tasks.tail.foreach(id => call(port, "COMPLETE", id).head shouldBe "SUCCEEDED")
      Thread.sleep(80)
    }
    child.expect(marker, 5.seconds)
    child.transcript.asScala.exists(_.contains("WORKFLOW_DONE")) shouldBe false
  }
  private def business(port: Int, kind: String): Unit = {
    val ledger = call(port, "BUSINESS")
    ledger.head shouldBe "BUSINESS"
    if (kind == "bank") ledger.slice(1, 5) shouldBe Vector("3", "3", "0", "300")
    else ledger.slice(5, 7) shouldBe Vector("2", "2")
    ledger(7) shouldBe "0"
  }

  "The complete public workflow" should {
    for (kind <- Vector("bank", "fab")) {
      s"settle $kind fully including required downstream children and resource closure" in {
        val root = directory(s"monarch-r2-$kind-normal-")
        var service: Child = null; var first: Child = null
        try {
          val started = external(root); service = started._1; val port = started._2
          first = host(root, port, kind, "first")
          finishTasks(port, first)
          business(port, kind)
          println(s"R2_JVM kind=$kind scenario=normal business=verified evidence=$root")
        } finally { if (first != null) first.stop(); if (service != null) service.stop() }
      }

      s"recover $kind after killing its coordinator while an external task remains ongoing" in {
        val root = directory(s"monarch-r2-$kind-recovery-")
        var service: Child = null; var first: Child = null; var second: Child = null
        try {
          val started = external(root); service = started._1; val port = started._2
          first = host(root, port, kind, "first")
          service.expect("EXTERNAL_SUBMIT ")
          first.transcript.asScala.exists(_.contains("WORKFLOW_DONE")) shouldBe false
          first.stop()
          second = host(root, port, kind, "recover")
          finishTasks(port, second)
          business(port, kind)
          println(s"R2_JVM kind=$kind scenario=ongoing-host-kill business=verified evidence=$root")
        } finally { if (first != null) first.stop(); if (second != null) second.stop(); if (service != null) service.stop() }
      }
    }

    val boundaries = Vector(
      ("Accepted", "delivery", false),
      ("ChildStartIntent", "delivery", false),
      ("ChildTerminalRecorded", "delivery", true),
      ("GrantRecorded", "resource", false),
      ("ReleaseRecorded", "resource", true),
      ("Settled", "job", true))
    for {
      kind <- Vector("bank", "fab")
      (event, owner, needsResult) <- boundaries ++ (if (kind == "fab") Vector(("TransferRecorded", "resource", true)) else Vector.empty)
      forced <- Vector(false, true)
    } {
      s"recover $kind at $event ${if (forced) "after force without ACK" else "before commit"}" in {
        val root = directory(s"monarch-r2-$kind-$event-$forced-")
        var service: Child = null; var first: Child = null; var second: Child = null
        try {
          val started = external(root); service = started._1; val port = started._2
          val job = "workflow-" + kind
          val entity = owner match {
            case "delivery" => job + "/batch-0/delivery"
            case "resource" => "domain"
            case "job" => job
          }
          val persistenceId = "r2-" + kind + "|" + owner + "|" + entity
          first = host(root, port, kind, "first", event, forced, persistenceId)
          if (needsResult) reachGate(port, first, event, forced)
          else {
            first.expect(s"HELD $event forced=$forced")
            call(port, "COUNT")(2) shouldBe "0" // No operation may precede admission/start intent ACK.
          }
          val path = SerializedValidationJournal.journalPath(root.resolve("host"), persistenceId)
          val records = SerializedValidationJournal.readFile(path)
          val gated = records.exists { r =>
            val alternative = r.serializerId match {
              case 77102140 => resource.proto.ResourceEventPO.parseFrom(r.bytes).event.productPrefix
              case 77102150 => delivery.proto.DeliveryEventPO.parseFrom(r.bytes).event.productPrefix
              case 77102160 => batch.proto.BatchEventPO.parseFrom(r.bytes).event.productPrefix
              case other => fail(s"unexpected owner serializer $other")
            }
            alternative == event
          }
          gated shouldBe forced
          first.stop()
          second = host(root, port, kind, "recover")
          finishTasks(port, second)
          business(port, kind)
          // Recover the completed parent again: no observer or external re-execution is needed.
          second.stop()
          val before = call(port, "COUNT")
          second = host(root, port, kind, "terminal-recover")
          second.expect("WORKFLOW_DONE")
          second.finish()
          call(port, "COUNT") shouldBe before
          business(port, kind)
          println(s"R2_JVM kind=$kind scenario=$event forced=$forced business=verified resources=closed evidence=$root")
        } finally { if (first != null) first.stop(); if (second != null) second.stop(); if (service != null) service.stop() }
      }
    }

    for (forced <- Vector(false, true)) {
      s"replay a sealed bank manifest ${if (forced) "after durable force without ACK" else "before commit"} without losing input" in {
        val root = directory(s"monarch-r2-seal-$forced-")
        var service: Child = null; var first: Child = null; var second: Child = null
        try {
          val started = external(root); service = started._1; val port = started._2
          first = host(root, port, "bank", "first", "Sealed", forced)
          first.expect(s"HELD Sealed forced=$forced")
          call(port, "COUNT")(2) shouldBe "0"
          first.stop()
          second = host(root, port, "bank", "recover")
          finishTasks(port, second)
          business(port, "bank")
          println(s"R2_JVM kind=bank scenario=sealed forced=$forced business=verified evidence=$root")
        } finally { if (first != null) first.stop(); if (second != null) second.stop(); if (service != null) service.stop() }
      }
    }
  }
}
