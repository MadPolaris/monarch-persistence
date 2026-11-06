package net.imadz.monarch.persistence

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

/** Real independent JVMs: external task history survives host death. The business
  * oracle is the external service's durable effect ledger, never an observer counter.
  */
class OperationJvmRecoverySpec extends AnyWordSpec with Matchers {
  import RecoveryProcessFixture._
  private val serviceMain = "net.imadz.monarch.persistence.ExternalTaskServiceFixture"
  private val hostMain = "net.imadz.monarch.persistence.ManagedOperationJvmHost"

  private def external(root: Path): (Child, Int) = {
    val dir = Files.createDirectory(root.resolve("external"))
    val child = start(serviceMain, Vector(dir.toString), root, "external", getClass.getClassLoader)
    val ready = child.expect("EXTERNAL_READY ")
    child -> ready.substring(ready.indexOf("EXTERNAL_READY ") + "EXTERNAL_READY ".length).trim.toInt
  }
  private def host(root: Path, port: Int, kind: String, mode: String, held: String = "", forced: Boolean = false): Child = {
    val dir = root.resolve("host")
    Files.createDirectories(dir)
    start(hostMain, Vector(dir.toString, port.toString, kind, "r1-" + kind, mode, held, forced.toString),
      root, mode, getClass.getClassLoader)
  }
  private def call(port: Int, name: String, fields: String*): Vector[String] =
    ExternalTaskServiceFixture.call(port, name, fields.toVector)
  private def op(kind: String): String = "r1-" + kind + "/submit"
  private def completed(port: Int, kind: String): Unit = {
    call(port, "COMPLETE", op(kind)).head shouldBe "SUCCEEDED"
    val count = call(port, "COUNT")
    count.head shouldBe "COUNT"
    count(1) shouldBe "1"
  }
  private def assertOneEffect(port: Int): Unit = {
    val count = call(port, "COUNT")
    count(1) shouldBe "1"
    count(2) shouldBe "1"
    count(3) shouldBe "1" // diagnostics: service was not restarted during host recovery
  }
  private def noNext(child: Child): Unit =
    child.transcript.asScala.exists(_.contains("HOST_NEXT")) shouldBe false

  "A recoverable bank or Fab operation" should {
    for (kind <- Vector("bank", "fab")) {
      s"recover $kind after acceptance reply loss without submitting another effect" in {
        val root = directory(s"monarch-r1-$kind-acceptance-")
        var ext: Child = null; var first: Child = null; var second: Child = null
        try {
          val started = external(root); ext = started._1; val port = started._2
          call(port, "DROP_NEXT_SUBMIT_REPLY").head shouldBe "OK"
          first = host(root, port, kind, "first")
          ext.expect("EXTERNAL_SUBMIT " + op(kind))
          first.expect("HOST_OPERATION Ongoing")
          noNext(first)
          first.stop()
          completed(port, kind) // external completion happens with no live host
          second = host(root, port, kind, "recover")
          second.expect("HOST_DONE")
          second.finish()
          assertOneEffect(port)
          second.transcript.asScala.count(_.contains("HOST_NEXT")) shouldBe 1
          println(s"R1_JVM kind=$kind scenario=acceptance-lost effects=1 submits=1 evidence=$root")
        } finally { if (first != null) first.stop(); if (second != null) second.stop(); if (ext != null) ext.stop() }
      }

      s"resume $kind Ongoing work and not treat a restarted host as a new business attempt" in {
        val root = directory(s"monarch-r1-$kind-ongoing-")
        var ext: Child = null; var first: Child = null; var second: Child = null
        try {
          val started = external(root); ext = started._1; val port = started._2
          first = host(root, port, kind, "first")
          first.expect("HOST_OPERATION Ongoing")
          noNext(first)
          first.stop()
          second = host(root, port, kind, "recover")
          second.expect("HOST_OPERATION Ongoing")
          noNext(second)
          completed(port, kind)
          second.expect("HOST_DONE")
          second.finish()
          assertOneEffect(port)
          println(s"R1_JVM kind=$kind scenario=ongoing effects=1 submits=1 evidence=$root")
        } finally { if (first != null) first.stop(); if (second != null) second.stop(); if (ext != null) ext.stop() }
      }

      for (forced <- Vector(false, true)) {
        s"recover $kind when external completion precedes the ${if (forced) "forced-but-unacknowledged" else "uncommitted"} operation terminal" in {
          val root = directory(s"monarch-r1-$kind-terminal-$forced-")
          var ext: Child = null; var first: Child = null; var second: Child = null
          try {
            val started = external(root); ext = started._1; val port = started._2
            first = host(root, port, kind, "first", "event:TerminalRecorded", forced)
            first.expect("HOST_OPERATION Ongoing")
            completed(port, kind)
            first.expect(s"HELD TerminalRecorded forced=$forced")
            noNext(first)
            first.stop()
            second = host(root, port, kind, "recover")
            second.expect("HOST_DONE")
            second.finish()
            assertOneEffect(port)
            println(s"R1_JVM kind=$kind scenario=operation-terminal forced=$forced effects=1 submits=1 evidence=$root")
          } finally { if (first != null) first.stop(); if (second != null) second.stop(); if (ext != null) ext.stop() }
        }
      }

      for (forced <- Vector(false, true)) {
        s"recover $kind with terminal committed but stage checkpoint ${if (forced) "forced without ACK" else "not committed"}" in {
          val root = directory(s"monarch-r1-$kind-checkpoint-$forced-")
          var ext: Child = null; var first: Child = null; var second: Child = null
          try {
            val started = external(root); ext = started._1; val port = started._2
            first = host(root, port, kind, "first", "cursor:submit#0", forced)
            first.expect("HOST_OPERATION Ongoing")
            completed(port, kind)
            first.expect(s"HELD submit#0 forced=$forced")
            noNext(first)
            first.stop()
            second = host(root, port, kind, "recover")
            second.expect("HOST_DONE")
            second.finish()
            assertOneEffect(port)
            second.transcript.asScala.count(_.contains("HOST_NEXT")) shouldBe 1
            println(s"R1_JVM kind=$kind scenario=checkpoint forced=$forced effects=1 submits=1 evidence=$root")
          } finally { if (first != null) first.stop(); if (second != null) second.stop(); if (ext != null) ext.stop() }
        }
      }

      s"block $kind after external history loss instead of interpreting NotFound as safe to resend" in {
        val root = directory(s"monarch-r1-$kind-history-loss-")
        var ext: Child = null; var first: Child = null; var second: Child = null
        try {
          val started = external(root); ext = started._1; val port = started._2
          first = host(root, port, kind, "first")
          first.expect("HOST_OPERATION Ongoing")
          first.stop()
          completed(port, kind)
          call(port, "FORGET").head shouldBe "EPOCH"
          second = host(root, port, kind, "recover")
          second.expect("HOST_OPERATION Blocked")
          noNext(second)
          call(port, "COUNT")(1) shouldBe "1"
          call(port, "COUNT")(3) shouldBe "1"
          second.stop()
          // A further restart does not reset the persisted unknown budget or silently resubmit.
          second = host(root, port, kind, "recover-again")
          second.expect("HOST_OPERATION Blocked")
          noNext(second)
          call(port, "COUNT")(1) shouldBe "1"
          println(s"R1_JVM kind=$kind scenario=history-loss blocked=true evidence=$root")
        } finally { if (first != null) first.stop(); if (second != null) second.stop(); if (ext != null) ext.stop() }
      }
    }
  }
}
