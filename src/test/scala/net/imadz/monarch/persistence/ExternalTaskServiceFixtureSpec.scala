package net.imadz.monarch.persistence

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Files
import scala.jdk.CollectionConverters._

class ExternalTaskServiceFixtureSpec extends AnyWordSpec with Matchers {
  import RecoveryProcessFixture._

  "The independent external task oracle" should {
    "recover its accepted tasks and completed effects from disk and preserve effects when history is forgotten" in {
      val root = directory("monarch-r1-external-ledger-")
      val ledger = Files.createDirectory(root.resolve("ledger"))
      var child: Child = null
      def launch(): Int = {
        child = start("net.imadz.monarch.persistence.ExternalTaskServiceFixture", Vector(ledger.toString), root,
          "external-restart", getClass.getClassLoader)
        child.expect("EXTERNAL_READY ").stripPrefix("EXTERNAL_READY ").trim.toInt
      }
      try {
        var port = launch()
        val request = Vector("bank", "operation-1", "frozen-digest", "ledger-1", "credit-1", "CREDIT:100")
        ExternalTaskServiceFixture.call(port, "SUBMIT", request) shouldBe Vector("RUNNING", "task-1", "ledger-1")
        ExternalTaskServiceFixture.call(port, "SUBMIT", request) shouldBe Vector("RUNNING", "task-1", "ledger-1")
        ExternalTaskServiceFixture.call(port, "SUBMIT", request.updated(5, "CREDIT:999")).head shouldBe "REJECTED"
        ExternalTaskServiceFixture.call(port, "COMPLETE", Vector("operation-1")).head shouldBe "SUCCEEDED"
        ExternalTaskServiceFixture.call(port, "COMPLETE", Vector("operation-1")).head shouldBe "SUCCEEDED"
        ExternalTaskServiceFixture.call(port, "COUNT")(1) shouldBe "1"
        child.stop()
        port = launch()
        ExternalTaskServiceFixture.call(port, "QUERY", Vector("operation-1")) shouldBe
          Vector("SUCCEEDED", "task-1", "ledger-1", "bank-receipt:operation-1", "operation-1", "frozen-digest")
        ExternalTaskServiceFixture.call(port, "COUNT").take(3) shouldBe Vector("COUNT", "1", "1")
        child.transcript.asScala.exists(_.contains("EXTERNAL_EFFECT")) shouldBe false
        ExternalTaskServiceFixture.call(port, "FORGET") shouldBe Vector("EPOCH", "ledger-2")
        ExternalTaskServiceFixture.call(port, "QUERY", Vector("operation-1")) shouldBe Vector("NOT_FOUND", "ledger-2")
        ExternalTaskServiceFixture.call(port, "COUNT").take(3) shouldBe Vector("COUNT", "1", "0")
        ExternalTaskServiceFixture.call(port, "SUBMIT", request.updated(3, "ledger-2")).head shouldBe "REJECTED"
        child.stop()
        port = launch()
        ExternalTaskServiceFixture.call(port, "COUNT").take(3) shouldBe Vector("COUNT", "1", "0")
        ExternalTaskServiceFixture.call(port, "EPOCH") shouldBe Vector("EPOCH", "ledger-2")
        println(s"R1_EXTERNAL_LEDGER restarts=2 effects=1 history-loss-preserves-effects=true evidence=$root")
      } finally if (child != null) child.stop()
    }
  }
}
