package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.batch._
import net.imadz.monarch.persistence.delivery.DeliveryProtocol
import net.imadz.monarch.persistence.resource.{ResourceCoordinator, ResourceEvidenceValidator, ResourceProtocol}
import net.imadz.monarch.persistence.workflow._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import scala.concurrent.duration._

class BatchCoordinatorSpec extends ScalaTestWithActorTestKit(SerializedValidationJournal.config.withFallback(ConfigFactory.load()))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
  private val proof = ByteString("verified-result")
  private val evidence = new ResourceEvidenceValidator {
    override def validate(grant: ResourceProtocol.Grant, supplied: ResourceProtocol.Proof): Either[String, Unit] =
      if (supplied.evidence == proof) Right(()) else Left("unknown cannot release resources")
  }
  private val bankAreas = Vector(
    AreaDefinition("recharge", "1", BatchPolicy.countOrWait(3, 100), Map("ftp" -> 1), maxExecutions = 3),
    AreaDefinition("purchase", "1", BatchPolicy.single, Map("legacy" -> 1), maxExecutions = 1))
  private val settings = BatchSettings(maxAcceptedItems = 20, maxActiveItems = 6, maxActiveBytes = 32768,
    pollIntervalMillis = 20, callTimeoutMillis = 2000)
  private def submission(m: BatchManifest): RunSubmission = RunSubmission(m.runId, m.area, "1", "1", BatchCodec.manifestToBytes(m))
  private def completed(r: DeliveryProtocol.Request, completion: BatchCompletion): DeliveryProtocol.Current =
    DeliveryProtocol.Current(DeliveryProtocol.View(r.deliveryId, Some(r.fingerprint), Some(r.child.runId), "Completed",
      Some(BatchCodec.completionToBytes(completion)), None))
  private def classifier: BatchExecutionPort = new BatchExecutionPort {
    override def prepare(m: BatchManifest, grant: ResourceProtocol.Grant): Either[String, RunSubmission] = Right(submission(m))
    override def classify(m: BatchManifest, result: ByteString): Either[String, BatchCompletion] = Right(BatchCodec.completionFromBytes(result))
  }
  private def view(ref: ActorRef[BatchProtocol.Command]): BatchProtocol.View = {
    val p = createTestProbe[BatchProtocol.Reply]()
    ref ! BatchProtocol.Get(p.ref)
    p.receiveMessage().asInstanceOf[BatchProtocol.Current].view
  }
  private def offer(ref: ActorRef[BatchProtocol.Command], n: Int, area: String = "recharge", key: String = "fund"): Unit = {
    val p = createTestProbe[BatchProtocol.Reply]()
    ref ! BatchProtocol.Offer(n.toLong, s"item-$n", ByteString(s"item-$n"), area, key, p.ref)
    p.expectMessage(BatchProtocol.Accepted(n.toLong, s"item-$n", duplicate = false))
  }
  private def close(ref: ActorRef[BatchProtocol.Command]): Unit = {
    val p = createTestProbe[BatchProtocol.Reply]()
    ref ! BatchProtocol.CloseInput(p.ref)
    p.receiveMessage() shouldBe a[BatchProtocol.Current]
  }

  "Durable batch coordination" should {
    "preserve successful identities while retrying only the failed subset and draining required purchases after EOF" in {
      val id = "batch-subset-" + UUID.randomUUID()
      val manifests = createTestProbe[BatchManifest]()
      def inbox: Behavior[DeliveryProtocol.Command] = Behaviors.receiveMessage {
        case DeliveryProtocol.Deliver(request, replyTo) =>
          val m = BatchCodec.manifestFromBytes(request.child.initialState)
          manifests.ref ! m
          val decisions = m.items.map { item =>
            val next = if (m.area == "purchase") ItemDisposition.Completed(Vector[Byte](1))
            else if (item.itemId == "item-1" && item.execution == 0) ItemDisposition.Retry(30)
            else ItemDisposition.Route("purchase", "purchase")
            ItemDecision(item.itemId, next)
          }
          replyTo ! completed(request, BatchCompletion(decisions, ResourceDisposition.Release(proof.toVector)))
          Behaviors.same
        case _ => Behaviors.same
      }
      val receiver = spawn(inbox)
      val resource = spawn(ResourceCoordinator.behavior(PersistenceId.ofUniqueId(id + "-resources"), Map("ftp" -> 1, "legacy" -> 1), evidence))
      val definition = BatchDefinition("bank", "1", bankAreas, settings)
      val ref = spawn(BatchCoordinator.behavior(PersistenceId.ofUniqueId(id), id, definition, resource, _ => receiver, classifier))
      (0 to 2).foreach(offer(ref, _))
      close(ref)
      createTestProbe[Unit]().awaitAssert(view(ref).phase shouldBe "Drained", 10.seconds)
      val result = view(ref)
      result.succeeded shouldBe 3
      result.failed shouldBe 0
      result.active shouldBe 0
      result.items.find(_.itemId == "item-1").get.executions shouldBe Map("recharge" -> 2, "purchase" -> 1)
      result.items.filterNot(_.itemId == "item-1").foreach(_.executions shouldBe Map("recharge" -> 1, "purchase" -> 1))
      testKit.stop(ref)
      val recovered = spawn(BatchCoordinator.behavior(PersistenceId.ofUniqueId(id), id, definition, resource, _ => receiver, classifier))
      view(recovered).phase shouldBe "Drained"
      val duplicate = createTestProbe[BatchProtocol.Reply]()
      recovered ! BatchProtocol.Offer(1, "item-1", ByteString("item-1"), "recharge", "fund", duplicate.ref)
      duplicate.expectMessage(BatchProtocol.Accepted(1, "item-1", duplicate = true))
      testKit.stop(recovered); testKit.stop(resource); testKit.stop(receiver)
    }

    "refuse missing duplicate and foreign results without settling or releasing the active batch" in {
      val id = "batch-conservation-" + UUID.randomUUID()
      val calls = createTestProbe[DeliveryProtocol.Request]()
      val receiver = spawn(Behaviors.receiveMessage[DeliveryProtocol.Command] {
        case DeliveryProtocol.Deliver(r, to) =>
          calls.ref ! r
          to ! completed(r, BatchCompletion(Vector(ItemDecision("foreign-item", ItemDisposition.Completed(Vector.empty))),
            ResourceDisposition.Release(proof.toVector)))
          Behaviors.same
        case _ => Behaviors.same
      })
      val resource = spawn(ResourceCoordinator.behavior(PersistenceId.ofUniqueId(id + "-resources"), Map("ftp" -> 1, "legacy" -> 1), evidence))
      val ref = spawn(BatchCoordinator.behavior(PersistenceId.ofUniqueId(id), id, BatchDefinition("bank", "1", bankAreas, settings),
        resource, _ => receiver, classifier))
      offer(ref, 0); close(ref)
      calls.receiveMessage()
      createTestProbe[Unit]().awaitAssert(view(ref).phase shouldBe "ClosingBlocked", 5.seconds)
      val status = view(ref)
      status.succeeded shouldBe 0
      status.active shouldBe 1
      val p = createTestProbe[ResourceProtocol.Reply]()
      resource ! ResourceProtocol.Query(status.batches.head.reservationId.get, p.ref)
      p.receiveMessage() shouldBe a[ResourceProtocol.Granted]
      testKit.stop(ref); testKit.stop(resource); testKit.stop(receiver)
    }

    "keep physical occupancy through a committed transfer and release it only after the destination child completes" in {
      val id = "batch-occupancy-" + UUID.randomUUID()
      val messages = createTestProbe[DeliveryProtocol.Deliver]()
      val receiver = spawn(Behaviors.receiveMessage[DeliveryProtocol.Command] {
        case message: DeliveryProtocol.Deliver => messages.ref ! message; Behaviors.same
        case _ => Behaviors.same
      })
      val capacities = Map("amhs" -> 1, "port" -> 1, "machine" -> 1)
      val resource = spawn(ResourceCoordinator.behavior(PersistenceId.ofUniqueId(id + "-resources"), capacities, evidence))
      val areas = Vector(AreaDefinition("move", "1", BatchPolicy.single, Map("amhs" -> 1, "port" -> 1)),
        AreaDefinition("process", "1", BatchPolicy.single, Map("machine" -> 1)))
      val ref = spawn(BatchCoordinator.behavior(PersistenceId.ofUniqueId(id), id, BatchDefinition("fab", "1", areas, settings),
        resource, _ => receiver, classifier))
      offer(ref, 0, "move"); close(ref)
      val move = messages.receiveMessage()
      val occupiedId = id + "/foup-occupied"
      move.replyTo ! completed(move.request, BatchCompletion(Vector(ItemDecision("item-0", ItemDisposition.Route("process", "foup"))),
        ResourceDisposition.Carry(occupiedId, id + "/foup", Map("port" -> 1), proof.toVector)))
      val process = messages.receiveMessage()
      val manifest = BatchCodec.manifestFromBytes(process.request.child.initialState)
      manifest.items.head.carried.head.reservationId shouldBe occupiedId
      val p = createTestProbe[ResourceProtocol.Reply]()
      resource ! ResourceProtocol.Reserve(ResourceProtocol.Request("competitor", "other-foup", Map("port" -> 1)), p.ref)
      p.expectMessage(ResourceProtocol.Queued("competitor"))
      view(ref).phase should not be "Drained"
      process.replyTo ! completed(process.request, BatchCompletion(Vector(ItemDecision("item-0", ItemDisposition.Completed(Vector.empty))),
        ResourceDisposition.Release(proof.toVector)))
      createTestProbe[Unit]().awaitAssert(view(ref).phase shouldBe "Drained", 5.seconds)
      resource ! ResourceProtocol.Query("competitor", p.ref)
      p.receiveMessage() shouldBe a[ResourceProtocol.Granted]
      testKit.stop(ref); testKit.stop(resource); testKit.stop(receiver)
    }

    "wait on a committed reconciliation grant across repeated Blocked observations" in {
      val id = "batch-reconciling-grant-" + UUID.randomUUID()
      val calls = createTestProbe[ResourceProtocol.Command]()
      val deliveries = createTestProbe[DeliveryProtocol.Command]()
      val ref = spawn(BatchCoordinator.behavior(PersistenceId.ofUniqueId(id), id,
        BatchDefinition("bank", "1", bankAreas, settings), calls.ref, _ => deliveries.ref, classifier))
      try {
        offer(ref, 0); close(ref)
        val reserve = calls.expectMessageType[ResourceProtocol.Reserve]
        reserve.replyTo ! ResourceProtocol.Granted(ResourceProtocol.Grant(reserve.request.reservationId,
          reserve.request.ownerOperationKey, "token", reserve.request.units))
        val delivery = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        val r = delivery.request
        val checking = DeliveryProtocol.Current(DeliveryProtocol.View(r.deliveryId, Some(r.fingerprint), Some(r.child.runId),
          "Blocked", None, Some("previous query budget exhausted"), reconciling = true))
        delivery.replyTo ! checking
        (1 to 3).foreach { _ =>
          val query = deliveries.expectMessageType[DeliveryProtocol.Get]
          view(ref).batches.head.phase shouldBe "WaitingChild"
          query.replyTo ! checking
        }
        val terminalQuery = deliveries.expectMessageType[DeliveryProtocol.Get]
        terminalQuery.replyTo ! completed(r, BatchCompletion(Vector(ItemDecision("item-0", ItemDisposition.Completed(Vector.empty))),
          ResourceDisposition.Release(proof.toVector)))
        val released = calls.expectMessageType[ResourceProtocol.Release]
        released.replyTo ! ResourceProtocol.Released(released.reservationId)
        createTestProbe[Unit]().awaitAssert(view(ref).phase shouldBe "Drained", 5.seconds)
      } finally testKit.stop(ref)
    }

    "reconcile an unacknowledged delivery by reattaching its frozen request before querying the child" in {
      val id = "batch-unaccepted-" + UUID.randomUUID()
      val calls = createTestProbe[ResourceProtocol.Command]()
      val deliveries = createTestProbe[DeliveryProtocol.Command]()
      val config = settings.copy(maxBatchCalls = 1, maxReconciliations = 3)
      val definition = BatchDefinition("bank", "1", bankAreas, config)
      val ref = spawn(BatchCoordinator.behavior(PersistenceId.ofUniqueId(id), id, definition,
        calls.ref, _ => deliveries.ref, classifier))
      val replies = createTestProbe[BatchProtocol.Reply]()
      try {
        offer(ref, 0); close(ref)
        val reserve = calls.expectMessageType[ResourceProtocol.Reserve]
        reserve.replyTo ! ResourceProtocol.Granted(ResourceProtocol.Grant(reserve.request.reservationId,
          reserve.request.ownerOperationKey, "token", reserve.request.units))
        // The only automatic call was admission. The frozen delivery exists,
        // but the inbox has not yet received it when this batch blocks.
        replies.awaitAssert(view(ref).phase shouldBe "ClosingBlocked", 5.seconds)
        deliveries.expectNoMessage(50.millis)
        val batchId = view(ref).batches.head.batchId
        ref ! BatchProtocol.Reconcile(batchId, replies.ref)
        replies.expectMessageType[BatchProtocol.Current]
        val first = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        first.request.deliveryId shouldBe batchId + "/delivery"
        val m = BatchCodec.manifestFromBytes(first.request.child.initialState)
        first.replyTo ! completed(first.request, BatchCompletion(Vector(ItemDecision("item-0", ItemDisposition.Completed(Vector.empty))),
          ResourceDisposition.Release(proof.toVector)))
        // Release needs its own finite round; no new delivery or operation is created.
        replies.awaitAssert(view(ref).phase shouldBe "ClosingBlocked", 5.seconds)
        ref ! BatchProtocol.Reconcile(batchId, replies.ref)
        replies.expectMessageType[BatchProtocol.Current]
        val release = calls.expectMessageType[ResourceProtocol.Release]
        release.reservationId shouldBe m.reservationId
        release.replyTo ! ResourceProtocol.Released(release.reservationId)
        replies.awaitAssert(view(ref).phase shouldBe "Drained", 5.seconds)
        view(ref).items.head.executions shouldBe Map("recharge" -> 1)
        deliveries.expectNoMessage(50.millis)
      } finally testKit.stop(ref)
    }

    "block a transfer reported as a release instead of dropping the carried claim" in {
      val id = "batch-wrong-transfer-" + UUID.randomUUID()
      val calls = createTestProbe[ResourceProtocol.Command]()
      val deliveries = createTestProbe[DeliveryProtocol.Command]()
      val areas = Vector(AreaDefinition("move", "1", BatchPolicy.single, Map("amhs" -> 1, "port" -> 1)),
        AreaDefinition("process", "1", BatchPolicy.single, Map("machine" -> 1)))
      val ref = spawn(BatchCoordinator.behavior(PersistenceId.ofUniqueId(id), id, BatchDefinition("fab", "1", areas, settings),
        calls.ref, _ => deliveries.ref, classifier))
      try {
        offer(ref, 0, "move"); close(ref)
        val reserve = calls.expectMessageType[ResourceProtocol.Reserve]
        reserve.replyTo ! ResourceProtocol.Granted(ResourceProtocol.Grant(reserve.request.reservationId,
          reserve.request.ownerOperationKey, "token", reserve.request.units))
        val move = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        move.replyTo ! completed(move.request, BatchCompletion(Vector(ItemDecision("item-0", ItemDisposition.Route("process", "foup"))),
          ResourceDisposition.Carry(id + "/occupancy", id + "/foup", Map("port" -> 1), proof.toVector)))
        val transfer = calls.expectMessageType[ResourceProtocol.Transfer]
        transfer.replyTo ! ResourceProtocol.Released(transfer.reservationId)
        createTestProbe[Unit]().awaitAssert(view(ref).phase shouldBe "ClosingBlocked", 5.seconds)
        val current = view(ref)
        current.active shouldBe 1
        current.succeeded shouldBe 0
        current.items.head.phase shouldBe "Running"
        current.items.head.area shouldBe "move"
        current.batches.head.phase shouldBe "Blocked"
        deliveries.expectNoMessage(50.millis)
      } finally testKit.stop(ref)
    }

    "enforce hard tail minima and bounded acceptance without reporting a false drain" in {
      val id = "batch-tail-" + UUID.randomUUID()
      val messages = createTestProbe[DeliveryProtocol.Command]()
      val resource = spawn(ResourceCoordinator.behavior(PersistenceId.ofUniqueId(id + "-resources"), Map("device" -> 1), evidence))
      val config = BatchDefinition("fab-hard", "1", Vector(AreaDefinition("hard", "1", BatchPolicy.compatible(2, 2, 10), Map("device" -> 1))),
        settings.copy(maxActiveItems = 2, maxAcceptedItems = 2))
      val ref = spawn(BatchCoordinator.behavior(PersistenceId.ofUniqueId(id), id, config, resource, _ => messages.ref, classifier))
      offer(ref, 0, "hard", "recipe-a"); offer(ref, 1, "hard", "recipe-b")
      close(ref)
      createTestProbe[Unit]().awaitAssert(view(ref).phase shouldBe "ClosingBlocked")
      messages.expectNoMessage(100.millis)
      view(ref).active shouldBe 2
      testKit.stop(ref); testKit.stop(resource)
    }
  }
}
