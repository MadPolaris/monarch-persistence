package net.imadz.monarch.persistence

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64
import scala.jdk.CollectionConverters._

/** Black-box domain expectations, deliberately independent of product batch codecs and host adapters. */
class DomainLedgerFixtureSpec extends AnyWordSpec with Matchers {
  import RecoveryProcessFixture._

  private def encoded(id: String): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(id.getBytes(StandardCharsets.UTF_8))

  private def payload(area: String, items: Vector[(String, Int)]): String =
    s"WF2:$area:" + items.map { case (id, execution) => s"${encoded(id)}~$execution" }.mkString(",")

  private def result(area: String, items: (String, String)*): String =
    s"WR2:$area:" + items.map { case (id, classification) => s"${encoded(id)}~$classification" }.mkString(",")

  private def rows(items: (String, String, String)*): String =
    items.sortBy(_._1).map { case (id, first, second) => s"${encoded(id)}~$first~$second" }.mkString(",")

  private def request(kind: String, key: String, businessPayload: String,
      epoch: String = "ledger-1"): Vector[String] =
    Vector(kind, key, s"digest-$key", epoch, s"effect-$key", businessPayload)

  private final class Oracle extends AutoCloseable {
    val root = directory("monarch-r2-domain-ledger-")
    private val ledger = Files.createDirectory(root.resolve("ledger"))
    private var child: Child = null
    private var port = 0

    def launch(): Unit = {
      child = start("net.imadz.monarch.persistence.ExternalTaskServiceFixture", Vector(ledger.toString),
        root, "domain-ledger", getClass.getClassLoader)
      port = child.expect("EXTERNAL_READY ").stripPrefix("EXTERNAL_READY ").trim.toInt
      child.transcript.asScala.exists(_.contains("EXTERNAL_EFFECT")) shouldBe false
    }

    def restart(): Unit = {
      child.stop()
      launch()
    }

    def call(command: String, fields: Vector[String] = Vector.empty): Vector[String] =
      ExternalTaskServiceFixture.call(port, command, fields)

    def submit(kind: String, key: String, area: String, items: Vector[(String, Int)],
        epoch: String = "ledger-1"): Vector[String] = {
      val response = call("SUBMIT", request(kind, key, payload(area, items), epoch))
      response.head shouldBe "RUNNING"
      response.size shouldBe 3
      response(2) shouldBe epoch
      response
    }

    def complete(key: String, expectedResult: String): Vector[String] = {
      val response = call("COMPLETE", Vector(key))
      response.head shouldBe "SUCCEEDED"
      response.size shouldBe 4
      response(3) shouldBe expectedResult
      response
    }

    def business(values: Long*): Unit =
      call("BUSINESS") shouldBe ("BUSINESS" +: values.map(_.toString).toVector)

    def count(effects: Int, accepted: Int): Unit =
      call("COUNT").take(3) shouldBe Vector("COUNT", effects.toString, accepted.toString)

    override def close(): Unit = if (child != null) child.stop()
  }

  private def withOracle(test: Oracle => Unit): Unit = {
    val oracle = new Oracle
    try {
      oracle.launch()
      test(oracle)
    } finally oracle.close()
  }

  "The independent WF2 domain ledger" should {
    "retain partial recharge, retry-subset, purchase and Fab rework effects through two process restarts" in {
      withOracle { oracle =>
        val customers = Vector("customer-0", "customer-1", "customer-2")
        val foups = Vector("foup-0", "foup-1")
        val allCustomers = customers.map(_ -> 0)
        val allFoups = foups.map(_ -> 0)
        val initialResult = result("recharge", "customer-0" -> "SUCCESS", "customer-1" -> "RETRY",
          "customer-2" -> "SUCCESS")
        val measuredResult = result("measure", "foup-0" -> "SUCCESS", "foup-1" -> "REWORK")
        val partialBank = rows(("customer-0", "100", "0"), ("customer-1", "0", "0"),
          ("customer-2", "100", "0"))
        val purchasedBank = rows(("customer-0", "0", "100"), ("customer-1", "0", "100"),
          ("customer-2", "0", "100"))
        val measuredFab = rows(("foup-0", "metrology", "accepted"),
          ("foup-1", "metrology", "measurement-rework"))
        val reworkedFab = rows(("foup-0", "metrology", "accepted"), ("foup-1", "metrology", "reworked"))

        oracle.business(0, 0, 0, 0, 0, 0, 0)
        oracle.call("TASKS") shouldBe Vector("TASKS")
        // Rejection must preserve this exact task; it can only complete after this customer's credit.
        val earlyPurchase = oracle.submit("bank", "purchase-one", "purchase", Vector("customer-1" -> 0))
        oracle.call("COMPLETE", Vector("purchase-one")) shouldBe
          Vector("REJECTED", "BUSINESS_ORDER_VIOLATION")
        oracle.call("QUERY", Vector("purchase-one")) shouldBe
          (earlyPurchase ++ Vector("purchase-one", "digest-purchase-one"))
        oracle.call("ITEMS") shouldBe Vector("ITEMS", "", "")

        oracle.submit("bank", "recharge-all", "recharge", allCustomers)
        val rechargeReceipt = oracle.complete("recharge-all", initialResult)
        oracle.call("COMPLETE", Vector("recharge-all")) shouldBe rechargeReceipt
        oracle.business(2, 0, 200, 0, 0, 0, 0)
        oracle.call("ITEMS") shouldBe Vector("ITEMS", partialBank, "")
        oracle.count(1, 2)
        oracle.call("COMPLETE", Vector("purchase-one")).head shouldBe "REJECTED"

        // The first item could now credit, but the second already credited in another batch.
        // A host-selected retry of successful items must reject the entire batch, with no partial credit.
        oracle.submit("bank", "bad-full-recharge", "recharge",
          Vector("customer-1" -> 1, "customer-0" -> 1))
        oracle.call("COMPLETE", Vector("bad-full-recharge")) shouldBe
          Vector("REJECTED", "BUSINESS_DUPLICATE_EXECUTION")
        oracle.call("ITEMS") shouldBe Vector("ITEMS", partialBank, "")
        val retryAccepted = oracle.submit("bank", "recharge-retry", "recharge", Vector("customer-1" -> 1))

        Vector(("transport-in", "litho", "imported"), ("litho", "litho", "processed"),
          ("transport-out", "metrology", "awaiting-measurement")).foreach { case (area, location, phase) =>
          oracle.submit("fab", s"fab-$area", area, allFoups)
          oracle.complete(s"fab-$area", result(area, "foup-0" -> "SUCCESS", "foup-1" -> "SUCCESS"))
          oracle.call("ITEMS")(2) shouldBe rows(("foup-0", location, phase), ("foup-1", location, phase))
        }
        oracle.submit("fab", "measure-all", "measure", allFoups)
        val measureReceipt = oracle.complete("measure-all", measuredResult)
        oracle.call("COMPLETE", Vector("measure-all")) shouldBe measureReceipt
        oracle.business(2, 0, 200, 0, 1, 0, 0)

        // The accepted first FOUP must not be unloaded if the second still requires rework.
        oracle.submit("fab", "unload-too-soon", "unload", allFoups)
        oracle.call("COMPLETE", Vector("unload-too-soon")) shouldBe
          Vector("REJECTED", "BUSINESS_ORDER_VIOLATION")
        oracle.call("ITEMS") shouldBe Vector("ITEMS", partialBank, measuredFab)
        val reworkAccepted = oracle.submit("fab", "rework-one", "rework", Vector("foup-1" -> 0))
        val running = Vector("TASKS", "bad-full-recharge", "purchase-one", "recharge-retry",
          "rework-one", "unload-too-soon")
        oracle.call("TASKS") shouldBe running
        oracle.count(5, 10)

        oracle.restart() // Completed mixed results and still-running retry/rework survive together.
        oracle.count(5, 10)
        oracle.business(2, 0, 200, 0, 1, 0, 0)
        oracle.call("TASKS") shouldBe running
        oracle.call("ITEMS") shouldBe Vector("ITEMS", partialBank, measuredFab)
        oracle.call("QUERY", Vector("recharge-all")) shouldBe
          (rechargeReceipt ++ Vector("recharge-all", "digest-recharge-all"))
        oracle.call("QUERY", Vector("measure-all")) shouldBe
          (measureReceipt ++ Vector("measure-all", "digest-measure-all"))
        oracle.call("QUERY", Vector("recharge-retry")) shouldBe
          (retryAccepted ++ Vector("recharge-retry", "digest-recharge-retry"))
        oracle.call("QUERY", Vector("rework-one")) shouldBe
          (reworkAccepted ++ Vector("rework-one", "digest-rework-one"))
        oracle.call("SUBMIT", request("bank", "recharge-all", payload("recharge", allCustomers))) shouldBe
          rechargeReceipt

        oracle.complete("recharge-retry", result("recharge", "customer-1" -> "SUCCESS"))
        oracle.business(3, 0, 300, 0, 1, 0, 0)
        val purchaseResult = result("purchase", "customer-1" -> "SUCCESS")
        oracle.complete("purchase-one", purchaseResult) shouldBe
          Vector("SUCCEEDED", earlyPurchase(1), earlyPurchase(2), purchaseResult)
        oracle.business(3, 1, 200, 100, 1, 0, 0)
        oracle.call("ITEMS")(1) shouldBe rows(("customer-0", "100", "0"),
          ("customer-1", "0", "100"), ("customer-2", "100", "0"))
        val remainingCustomers = Vector("customer-0" -> 0, "customer-2" -> 0)
        oracle.submit("bank", "purchase-rest", "purchase", remainingCustomers)
        val purchases = oracle.complete("purchase-rest",
          result("purchase", "customer-0" -> "SUCCESS", "customer-2" -> "SUCCESS"))
        oracle.call("COMPLETE", Vector("purchase-rest")) shouldBe purchases
        oracle.call("SUBMIT", request("bank", "purchase-rest", payload("purchase", remainingCustomers))) shouldBe
          purchases
        // Stored evidence must be returned, not reinterpreted against already-spent wallets.
        oracle.call("COMPLETE", Vector("recharge-all")) shouldBe rechargeReceipt

        val reworkReceipt = oracle.complete("rework-one", result("rework", "foup-1" -> "SUCCESS"))
        oracle.call("COMPLETE", Vector("rework-one")) shouldBe reworkReceipt
        oracle.submit("fab", "duplicate-measure", "measure", Vector("foup-1" -> 0))
        oracle.call("COMPLETE", Vector("duplicate-measure")) shouldBe
          Vector("REJECTED", "BUSINESS_DUPLICATE_EXECUTION")
        oracle.business(3, 3, 0, 300, 1, 0, 0)
        oracle.call("ITEMS") shouldBe Vector("ITEMS", purchasedBank, reworkedFab)
        oracle.count(9, 12)
        oracle.call("FORGET") shouldBe Vector("EPOCH", "ledger-2")
        oracle.call("TASKS") shouldBe Vector("TASKS")
        oracle.count(9, 0)
        oracle.business(3, 3, 0, 300, 1, 0, 0)

        oracle.restart() // Replay must retain domain history even though no old tasks remain visible.
        oracle.call("EPOCH") shouldBe Vector("EPOCH", "ledger-2")
        oracle.call("TASKS") shouldBe Vector("TASKS")
        oracle.count(9, 0)
        oracle.business(3, 3, 0, 300, 1, 0, 0)
        oracle.call("ITEMS") shouldBe Vector("ITEMS", purchasedBank, reworkedFab)
        oracle.call("QUERY", Vector("purchase-rest")) shouldBe Vector("NOT_FOUND", "ledger-2")
        oracle.call("SUBMIT", request("bank", "recharge-all", payload("recharge", allCustomers), "ledger-2")) shouldBe
          Vector("REJECTED", "OPERATION_HISTORY_UNAVAILABLE")
        oracle.call("SUBMIT", request("bank", "new-key-old-effect", payload("recharge", allCustomers), "ledger-2")
          .updated(4, "effect-recharge-all")) shouldBe Vector("REJECTED", "EFFECT_KEY_CONFLICT")

        oracle.submit("bank", "recharge-again", "recharge", Vector("customer-0" -> 32), "ledger-2")
        oracle.call("COMPLETE", Vector("recharge-again")) shouldBe
          Vector("REJECTED", "BUSINESS_DUPLICATE_EXECUTION")
        oracle.submit("bank", "purchase-again", "purchase", Vector("customer-0" -> 1), "ledger-2")
        oracle.call("COMPLETE", Vector("purchase-again")) shouldBe
          Vector("REJECTED", "BUSINESS_DUPLICATE_EXECUTION")
        oracle.business(3, 3, 0, 300, 1, 0, 0)
        oracle.count(9, 2)

        oracle.submit("fab", "measure-one", "measure", Vector("foup-1" -> 1), "ledger-2")
        oracle.call("COMPLETE", Vector("measure-one", result("measure", "foup-1" -> "REWORK"))) shouldBe
          Vector("REJECTED", "INVALID_FIELDS") // Callers cannot provide or override a classification.
        oracle.complete("measure-one", result("measure", "foup-1" -> "SUCCESS"))
        oracle.submit("fab", "unload-all", "unload", allFoups, "ledger-2")
        val unloaded = oracle.complete("unload-all", result("unload", "foup-0" -> "SUCCESS", "foup-1" -> "SUCCESS"))
        oracle.call("COMPLETE", Vector("unload-all")) shouldBe unloaded
        oracle.call("QUERY", Vector("unload-all")) shouldBe
          (unloaded ++ Vector("unload-all", "digest-unload-all"))
        oracle.submit("fab", "reimport", "transport-in", Vector("foup-0" -> 1), "ledger-2")
        oracle.call("COMPLETE", Vector("reimport")) shouldBe Vector("REJECTED", "BUSINESS_DUPLICATE_EXECUTION")
        oracle.submit("fab", "unload-again", "unload", Vector("foup-0" -> 1), "ledger-2")
        oracle.call("COMPLETE", Vector("unload-again")) shouldBe Vector("REJECTED", "BUSINESS_DUPLICATE_EXECUTION")
        oracle.call("TASKS") shouldBe
          Vector("TASKS", "purchase-again", "recharge-again", "reimport", "unload-again")
        oracle.call("ITEMS") shouldBe Vector("ITEMS", purchasedBank,
          rows(("foup-0", "stocker", "unloaded"), ("foup-1", "stocker", "unloaded")))
        oracle.business(3, 3, 0, 300, 2, 2, 0)
        oracle.count(11, 6)
        println(s"R2_DOMAIN_LEDGER restarts=2 credits=3 purchases=3 holdings=300 accepted=2 unloaded=2 duplicates=0 evidence=${oracle.root}")
      }
    }

    "validate WF2 before acceptance while preserving opaque payloads and bounding future result frames" in {
      withOracle { oracle =>
        val id = encoded("customer-0")
        val invalid = Vector(
          "WF2:recharge:",
          s"WF2:unknown:$id~0",
          s"WF2:recharge:$id",
          "WF2:recharge:~0",
          s"WF2:recharge:$id~",
          s"WF2:recharge:$id~-1",
          s"WF2:recharge:$id~33",
          s"WF2:recharge:$id~2147483648",
          s"WF2:recharge:$id~x",
          s"WF2:recharge:$id~0~1",
          s"WF2:recharge:$id~0:",
          s"WF2:recharge:$id~0,",
          s"WF2:recharge:$id~0,$id~1",
          "WF2:recharge:YQ~0,YQ==~1", // Same decoded identity despite optional padding.
          "WF2:recharge:a+b/~0",
          "WF2:recharge:Zh~0", // Noncanonical base64 trailing bits.
          "WF2:recharge:_w~0", // Invalid UTF-8, not an item with replacement characters.
          payload("recharge", (0 to 100).map(n => s"limit-$n" -> 0).toVector))
        invalid.zipWithIndex.foreach { case (value, index) =>
          withClue(s"invalid WF2 case $index: ") {
            oracle.call("SUBMIT", request("bank", s"invalid-$index", value)) shouldBe
              Vector("REJECTED", "INVALID_BUSINESS_PAYLOAD")
          }
        }
        oracle.call("SUBMIT", request("bank", "wrong-bank-area", s"WF2:litho:$id~0")) shouldBe
          Vector("REJECTED", "BUSINESS_KIND_MISMATCH")
        oracle.call("SUBMIT", request("fab", "wrong-fab-area", s"WF2:recharge:$id~0")) shouldBe
          Vector("REJECTED", "BUSINESS_KIND_MISMATCH")

        // The request is a legal wire frame, but its future SUCCESS result would exceed 65,536 bytes.
        val oversizedResult = payload("recharge", Vector(("x" * 49137) -> 0))
        oversizedResult.getBytes(StandardCharsets.UTF_8).length should be <= 65536
        oracle.call("SUBMIT", request("bank", "oversized-result", oversizedResult)) shouldBe
          Vector("REJECTED", "FRAME_LIMIT_EXCEEDED")
        Vector("BUSINESS", "ITEMS", "TASKS").foreach { command =>
          oracle.call(command, Vector("unexpected")) shouldBe Vector("REJECTED", "INVALID_FIELDS")
        }
        oracle.count(0, 0)
        oracle.business(0, 0, 0, 0, 0, 0, 0)
        oracle.call("TASKS") shouldBe Vector("TASKS")
        oracle.call("ITEMS") shouldBe Vector("ITEMS", "", "")

        Vector("bank" -> "bank-receipt", "fab" -> "fab-measurement").zipWithIndex.foreach {
          case ((kind, receipt), index) =>
            val key = s"opaque-$kind"
            oracle.call("SUBMIT", request(kind, key, "normalpayload")) shouldBe
              Vector("RUNNING", s"task-${index + 1}", "ledger-1")
            val completion = oracle.complete(key, s"$receipt:$key")
            oracle.call("COMPLETE", Vector(key)) shouldBe completion
        }
        oracle.count(2, 2)
        oracle.business(0, 0, 0, 0, 0, 0, 0)

        val batch = (0 until 100).map(n => s"bulk-$n" -> 0).toVector
        oracle.submit("bank", "hundred-items", "recharge", batch)
        val classifications = batch.zipWithIndex.map { case ((itemId, _), index) =>
          itemId -> (if (index % 10 == 1) "RETRY" else "SUCCESS")
        }
        val receipt = oracle.complete("hundred-items", result("recharge", classifications: _*))
        oracle.call("COMPLETE", Vector("hundred-items")) shouldBe receipt
        oracle.business(90, 0, 9000, 0, 0, 0, 0)
        oracle.count(3, 3)

        val unicodeId = "customer:~,/雪1"
        val paddedUnicode = Base64.getUrlEncoder.encodeToString(unicodeId.getBytes(StandardCharsets.UTF_8))
        oracle.call("SUBMIT", request("bank", "padded-utf8", s"WF2:recharge:YQ==~32,$paddedUnicode~0")).head shouldBe
          "RUNNING"
        oracle.complete("padded-utf8", result("recharge", "a" -> "SUCCESS", unicodeId -> "RETRY"))
        oracle.business(91, 0, 9100, 0, 0, 0, 0)
        oracle.count(4, 4)
        oracle.call("TASKS") shouldBe Vector("TASKS")
      }
    }
  }
}
