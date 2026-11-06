package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, ScalaTestWithActorTestKit}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.batch._
import net.imadz.monarch.persistence.operation.{ExternalOperationPort, OperationProtocol}
import net.imadz.monarch.persistence.resource.{ResourceEvidenceValidator, ResourceProtocol}
import net.imadz.monarch.persistence.workflow._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class WorkflowReconciliationSpec extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
  monarch.operation.max-checks = 1
  monarch.operation.manual-check-budget = 1
  monarch.operation.check-interval = 20ms
  monarch.delivery.max-polls = 3
  monarch.delivery.poll-interval = 500ms
""").withFallback(SerializedValidationJournal.config).withFallback(ConfigFactory.load()))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: ExecutionContext = system.executionContext

  "The bounded workflow host" should {
    for (restart <- Vector(false, true)) {
    s"resume the original operation after both budgets block, fresh host=$restart" in {
      val namespace = "reconcile-" + UUID.randomUUID()
      val ready = new AtomicBoolean(false)
      val submits = new AtomicInteger()
      val queries = new AtomicInteger()
      val program = new WorkflowProgram {
        override val definition = BatchDefinition("verify-only", "1", Vector(
          AreaDefinition("work", "1", BatchPolicy.single, Map("device" -> 1))),
          BatchSettings(maxAcceptedItems = 2, maxActiveItems = 2, maxActiveBytes = 8192,
            pollIntervalMillis = 20, callTimeoutMillis = 1000, maxBatchCalls = 100))
        override val capacities = Map("device" -> 1)
        override val resourceEvidence = new ResourceEvidenceValidator {
          override def validate(grant: ResourceProtocol.Grant, proof: ResourceProtocol.Proof): Either[String, Unit] =
            if (proof.evidence == ByteString("done")) Right(()) else Left("not proven")
        }
        override def operation(m: BatchManifest): OperationProtocol.Request = OperationProtocol.Request(
          m.operationKey, m.operationKey, "execute", "query-fixture", "1", "ledger-1", ByteString("immutable"), Some(m.reservationId))
        override def connector(area: String): ExternalOperationPort = new ExternalOperationPort {
          import OperationProtocol._
          override val connectorId = "query-fixture"
          override val connectorVersion = "1"
          def evidence(r: Request, payload: String): Evidence = Evidence(r.operationKey, r.fingerprint,
            r.ledgerEpoch, Some("original-task"), ByteString(payload))
          override def submit(r: Request): Future[Observation] = {
            submits.incrementAndGet()
            Future.successful(Accepted(evidence(r, "running")))
          }
          override def query(r: Request, prior: Option[Evidence]): Future[Observation] = {
            queries.incrementAndGet()
            Future.successful(if (ready.get()) Confirmed(Succeeded(ByteString("done"), evidence(r, "done")))
              else Running(evidence(r, "running")))
          }
          override def validateEvidence(r: Request, o: WithEvidence): Either[String, Unit] =
            if (o.evidence.taskId.contains("original-task")) Right(()) else Left("wrong task")
        }
        override def complete(m: BatchManifest, terminal: OperationProtocol.Terminal): Either[String, BatchCompletion] =
          Right(BatchCompletion(m.items.map(i => ItemDecision(i.itemId, ItemDisposition.Completed(terminal.result.toVector))),
            ResourceDisposition.Release(terminal.result.toVector)))
      }
      var kit = ActorTestKit("workflow-reconcile-" + UUID.randomUUID(), system.settings.config)
      var host = new WorkflowHost(kit.system, namespace, program)
      var job = host.job("job")
      var replies = kit.createTestProbe[BatchProtocol.Reply]()
      try {
      def current(): BatchProtocol.View = {
        job ! BatchProtocol.Get(replies.ref)
        replies.expectMessageType[BatchProtocol.Current].view
      }
      job ! BatchProtocol.Offer(0, "item", ByteString("payload"), "work", "key", replies.ref)
      replies.expectMessage(BatchProtocol.Accepted(0, "item", duplicate = false))
      job ! BatchProtocol.CloseInput(replies.ref)
      replies.expectMessageType[BatchProtocol.Current]
      replies.awaitAssert(current().phase shouldBe "ClosingBlocked", 5.seconds)
      submits.get() shouldBe 1
      queries.get() shouldBe 1
      current().succeeded shouldBe 0
      Await.result(host.reconcile("job", "foreign/batch-0"), 5.seconds) shouldBe a[BatchProtocol.Rejected]
      queries.get() shouldBe 1
      if (restart) {
        kit.shutdownTestKit()
        kit = ActorTestKit("workflow-reconcile-recovered-" + UUID.randomUUID(), system.settings.config)
        host = new WorkflowHost(kit.system, namespace, program)
        job = host.job("job")
        replies = kit.createTestProbe[BatchProtocol.Reply]()
        current().phase shouldBe "ClosingBlocked"
      }

      // No periodic polling or duplicate Offer can replenish the operation's budget.
      ready.set(true)
      job ! BatchProtocol.Offer(0, "item", ByteString("payload"), "work", "key", replies.ref)
      replies.expectMessage(BatchProtocol.Accepted(0, "item", duplicate = true))
      current().phase shouldBe "ClosingBlocked"
      Await.result(host.reconcile("job", "job/batch-0"), 5.seconds) shouldBe a[BatchProtocol.Current]
      replies.awaitAssert({
        val status = current()
        withClue(s"$status submits=${submits.get()} queries=${queries.get()}: ") { status.phase shouldBe "Drained" }
      }, 5.seconds)
      val result = current()
      result.succeeded shouldBe 1
      result.items.head.executions shouldBe Map("work" -> 1)
      submits.get() shouldBe 1
      queries.get() shouldBe 2
      val resource = createTestProbe[ResourceProtocol.Reply]()
      host.resourceDomain ! ResourceProtocol.Query("job/batch-0/resources", resource.ref)
      resource.expectMessage(ResourceProtocol.Released("job/batch-0/resources"))
      Await.result(host.reconcile("job", "job/batch-0"), 5.seconds) shouldBe a[BatchProtocol.Rejected]
      submits.get() shouldBe 1
      queries.get() shouldBe 2
      } finally kit.shutdownTestKit()
    }
    }
  }
}
