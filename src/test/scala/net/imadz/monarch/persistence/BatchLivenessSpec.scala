package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.{ActorRef, Behavior, BehaviorInterceptor, TypedActorContext}
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.PersistentRepr
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

class BatchLivenessSpec extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
  akka.actor.allow-java-serialization = off
  akka.persistence.journal.plugin = "controlled-journal"
  controlled-journal.class = "net.imadz.monarch.persistence.ControlledJournal"
  controlled-journal.plugin-dispatcher = "akka.actor.default-dispatcher"
  akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
  akka.actor.testkit.typed.single-expect-default = 10s
  akka.loglevel = WARNING
""").withFallback(ConfigFactory.load())) with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
  private val proof = ByteString("verified-liveness-result")
  private val settings = BatchSettings(maxAcceptedItems = 12, maxActiveItems = 6, maxActiveBytes = 4096,
    maxItemBytes = 64, maxConcurrentBatches = 1, pollIntervalMillis = 10, callTimeoutMillis = 10000)
  private val execution = new BatchExecutionPort {
    override def prepare(m: BatchManifest, grant: ResourceProtocol.Grant): Either[String, RunSubmission] =
      Right(RunSubmission(m.runId, m.area, "1", "1", BatchCodec.manifestToBytes(m)))
    override def classify(m: BatchManifest, result: ByteString): Either[String, BatchCompletion] =
      Right(BatchCodec.completionFromBytes(result))
  }
  private val evidence = new ResourceEvidenceValidator {
    override def validate(grant: ResourceProtocol.Grant, supplied: ResourceProtocol.Proof): Either[String, Unit] =
      if (supplied.evidence == proof) Right(()) else Left("physical completion evidence required")
  }

  private def view(ref: ActorRef[BatchProtocol.Command]): BatchProtocol.View = {
    val replies = createTestProbe[BatchProtocol.Reply]()
    ref ! BatchProtocol.Get(replies.ref)
    replies.expectMessageType[BatchProtocol.Current].view
  }
  private def offer(ref: ActorRef[BatchProtocol.Command], n: Int, area: String,
      key: String = "compatible", payload: ByteString = ByteString("item")): Unit = {
    val replies = createTestProbe[BatchProtocol.Reply]()
    ref ! BatchProtocol.Offer(n.toLong, s"item-$n", payload, area, key, replies.ref)
    replies.expectMessage(BatchProtocol.Accepted(n.toLong, s"item-$n", duplicate = false))
  }
  private def closeInput(ref: ActorRef[BatchProtocol.Command]): Unit = {
    val replies = createTestProbe[BatchProtocol.Reply]()
    ref ! BatchProtocol.CloseInput(replies.ref)
    replies.expectMessageType[BatchProtocol.Current].view.inputClosed shouldBe true
  }
  private def manifest(message: DeliveryProtocol.Deliver): BatchManifest = {
    // The envelope payload is a digest, not the manifest.
    message.request.payload shouldBe ByteString(message.request.child.fingerprint)
    BatchCodec.manifestFromBytes(message.request.child.initialState)
  }
  private def completed(message: DeliveryProtocol.Deliver, completion: BatchCompletion): Unit = {
    val r = message.request
    message.replyTo ! DeliveryProtocol.Current(DeliveryProtocol.View(r.deliveryId, Some(r.fingerprint),
      Some(r.child.runId), "Completed", Some(BatchCodec.completionToBytes(completion)), None))
  }
  private def success(m: BatchManifest): BatchCompletion = BatchCompletion(
    m.items.map(i => ItemDecision(i.itemId, ItemDisposition.Completed(Vector[Byte](1)))),
    ResourceDisposition.Release(proof.toVector))
  private def grant(request: ResourceProtocol.Reserve): Unit = request.replyTo ! ResourceProtocol.Granted(
    ResourceProtocol.Grant(request.request.reservationId, request.request.ownerOperationKey, "test-grant", request.request.units))
  private def release(calls: TestProbe[ResourceProtocol.Command], id: String): Unit = {
    val request = calls.expectMessageType[ResourceProtocol.Release]
    request.reservationId shouldBe id
    request.proof.evidence shouldBe proof
    request.replyTo ! ResourceProtocol.Released(id)
  }
  private def query(resources: ActorRef[ResourceProtocol.Command], id: String): ResourceProtocol.Reply = {
    val replies = createTestProbe[ResourceProtocol.Reply]()
    resources ! ResourceProtocol.Query(id, replies.ref)
    replies.receiveMessage()
  }
  private def durable(id: PersistenceId): BatchState =
    ControlledJournal.storage.getOrElse(id.id, Vector.empty).foldLeft(BatchState()) { (state, event) =>
      BatchState.evolve(state, event.payload.asInstanceOf[BatchEvent])
    }
  private def drained(ref: ActorRef[BatchProtocol.Command], count: Int): BatchProtocol.View = {
    createTestProbe[Unit]().awaitAssert({
      val current = view(ref)
      current.phase shouldBe "Drained"
      current.succeeded shouldBe count
      current.failed shouldBe 0
      current.held shouldBe 0
      current.active shouldBe 0
      current.activeBytes shouldBe 0L
      current.batches shouldBe empty
    }, 5.seconds)
    view(ref)
  }
  private def clear(id: PersistenceId): Unit = {
    ControlledJournal.probes.remove(id.id)
    ControlledJournal.storage.remove(id.id)
  }

  "Batch liveness" should {
    "leave the only slot for a delayed carried continuation and retain occupancy until real transport completes" in {
      val id = PersistenceId.ofUniqueId("batch-carried-delay-" + UUID.randomUUID())
      val resourceId = PersistenceId.ofUniqueId(id.id + "-resources")
      val calls = createTestProbe[ResourceProtocol.Command]()
      val deliveries = createTestProbe[DeliveryProtocol.Command]()
      val settlements = createTestProbe[ControlledJournal.Pending]()
      val journal = spawn(Behaviors.receiveMessage[ControlledJournal.Pending] { pending =>
        val delayedTransport = pending.events.exists(_.payload match {
          case BatchEvent.Settled(_, records) => records.exists(r =>
            r.item.itemId == "item-0" && r.item.area == "transport" && r.phase == "Pending")
          case _ => false
        })
        if (delayedTransport) settlements.ref ! pending else pending.release()
        Behaviors.same
      })
      ControlledJournal.probes.put(id.id, journal)
      val resources = spawn(ResourceCoordinator.behavior(resourceId,
        Map("amhs" -> 1, "port" -> 1, "machine" -> 1), evidence))
      val observedResources = spawn(Behaviors.receiveMessage[ResourceProtocol.Command] { command =>
        calls.ref ! command
        resources ! command
        Behaviors.same
      })
      val definition = BatchDefinition("carried-delay", "1", Vector(
        AreaDefinition("intake", "1", BatchPolicy.single, Map("amhs" -> 1, "port" -> 1)),
        AreaDefinition("process", "1", BatchPolicy.single, Map("machine" -> 1)),
        AreaDefinition("transport", "1", BatchPolicy.single, Map("amhs" -> 1))), settings)
      def coordinator: Behavior[BatchProtocol.Command] =
        BatchCoordinator.behavior(id, id.id, definition, observedResources, _ => deliveries.ref, execution)
      var ref = spawn(coordinator)
      try {
        offer(ref, 0, "intake")
        val intake = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        val intakeManifest = manifest(intake)
        calls.expectMessageType[ResourceProtocol.Reserve].request.reservationId shouldBe intakeManifest.reservationId
        val occupiedId = id.id + "/occupied-port"
        completed(intake, BatchCompletion(Vector(ItemDecision("item-0", ItemDisposition.Route("process", "carrier"))),
          ResourceDisposition.Carry(occupiedId, id.id + "/carrier", Map("port" -> 1), proof.toVector)))
        calls.expectMessageType[ResourceProtocol.Transfer].toReservationId shouldBe occupiedId
        val process = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        val processManifest = manifest(process)
        calls.expectMessageType[ResourceProtocol.Reserve].request.reservationId shouldBe processManifest.reservationId
        val claim = processManifest.items.head.carried.head
        claim.reservationId shouldBe occupiedId

        // Fresh intake needs the occupied port. Sealing it into the only slot
        // would prevent the later transport from ever releasing that port.
        offer(ref, 1, "intake")
        closeInput(ref)
        val delay = 2.seconds.toMillis
        completed(process, BatchCompletion(Vector(ItemDecision("item-0", ItemDisposition.Route("transport", "carrier", delay))),
          ResourceDisposition.Release(proof.toVector), retainCarried = true))
        calls.expectMessageType[ResourceProtocol.Release].reservationId shouldBe processManifest.reservationId
        val settlement = settlements.receiveMessage()
        val continuation = settlement.events.head.payload.asInstanceOf[BatchEvent.Settled].records.head
        continuation.item.carried shouldBe Vector(claim)
        continuation.dueAt - continuation.acceptedAt shouldBe delay
        query(resources, occupiedId) shouldBe ResourceProtocol.Granted(
          ResourceProtocol.Grant(claim.reservationId, claim.ownerOperationKey, claim.token, claim.units))
        durable(id).records(1L).phase shouldBe "Pending"
        System.currentTimeMillis() should be < continuation.dueAt
        settlement.release()

        // Observe the very next resource command, not just eventual success:
        // the old scheduler instead reserves another intake and deadlocks.
        val transportReservation = calls.expectMessageType[ResourceProtocol.Reserve]
        transportReservation.request.units shouldBe Map("amhs" -> 1)
        System.currentTimeMillis() should be >= continuation.dueAt
        val transport = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        val transportManifest = manifest(transport)
        transportManifest.area shouldBe "transport"
        transportManifest.items.map(_.itemId) shouldBe Vector("item-0")
        transportManifest.items.head.carried shouldBe Vector(claim)
        transportReservation.request.reservationId shouldBe transportManifest.reservationId
        val waiting = view(ref)
        waiting.batches.map(_.area) shouldBe Vector("transport")
        waiting.items.find(_.itemId == "item-1").get.phase shouldBe "Pending"
        waiting.active shouldBe 2
        query(resources, occupiedId) shouldBe ResourceProtocol.Granted(
          ResourceProtocol.Grant(claim.reservationId, claim.ownerOperationKey, claim.token, claim.units))

        completed(transport, success(transportManifest))
        val releasedClaim = calls.expectMessageType[ResourceProtocol.Release]
        releasedClaim.reservationId shouldBe occupiedId
        releasedClaim.token shouldBe claim.token
        releasedClaim.proof.operationKey shouldBe claim.ownerOperationKey
        calls.expectMessageType[ResourceProtocol.Release].reservationId shouldBe transportManifest.reservationId
        val freshReservation = calls.expectMessageType[ResourceProtocol.Reserve]
        freshReservation.request.units shouldBe Map("amhs" -> 1, "port" -> 1)
        val fresh = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        val freshManifest = manifest(fresh)
        freshManifest.items.map(_.itemId) shouldBe Vector("item-1")
        freshManifest.items.head.carried shouldBe empty
        completed(fresh, success(freshManifest))
        calls.expectMessageType[ResourceProtocol.Release].reservationId shouldBe freshManifest.reservationId
        val result = drained(ref, 2)
        result.items.find(_.itemId == "item-0").get.executions shouldBe
          Map("intake" -> 1, "process" -> 1, "transport" -> 1)
        query(resources, occupiedId) shouldBe ResourceProtocol.Released(occupiedId)
        durable(id).invalid shouldBe None
        testKit.stop(ref)
        ref = spawn(coordinator)
        view(ref) shouldBe result
      } finally {
        testKit.stop(ref); testKit.stop(observedResources); testKit.stop(resources); testKit.stop(journal)
        clear(id); clear(resourceId)
      }
    }

    "flush countOrWait tails when the next maximum-sized legal offer cannot fit in the remaining bytes" in {
      val id = PersistenceId.ofUniqueId("batch-byte-pressure-" + UUID.randomUUID())
      val calls = createTestProbe[ResourceProtocol.Command]()
      val deliveries = createTestProbe[DeliveryProtocol.Command]()
      val bounded = settings.copy(maxItemBytes = 8, maxActiveBytes = 32)
      val definition = BatchDefinition("byte-pressure", "1", Vector(
        AreaDefinition("tail", "1", BatchPolicy.countOrWait(3, 1.hour.toMillis), Map("worker" -> 1))), bounded)
      val ref = spawn(BatchCoordinator.behavior(id, id.id, definition, calls.ref, _ => deliveries.ref, execution))
      try {
        // Two compatibility groups of two: neither count, EOF, item pressure,
        // nor the one-hour wait can explain a seal at 28 occupied bytes.
        (0 to 3).foreach(n => offer(ref, n, "tail", if (n < 2) "a" else "b", ByteString(new Array[Byte](7))))
        val firstReservation = calls.expectMessageType[ResourceProtocol.Reserve]
        val before = view(ref)
        before.inputClosed shouldBe false
        before.active shouldBe 4
        before.active should be < bounded.maxActiveItems
        before.activeBytes shouldBe 28L
        before.activeBytes should be < bounded.maxActiveBytes.toLong
        before.activeBytes + bounded.maxItemBytes should be > bounded.maxActiveBytes.toLong
        val replies = createTestProbe[BatchProtocol.Reply]()
        val next = BatchProtocol.Offer(4L, "item-4", ByteString(new Array[Byte](8)), "tail", "c", replies.ref)
        ref ! next
        replies.expectMessageType[BatchProtocol.Backpressured]
        view(ref).accepted shouldBe 4L

        grant(firstReservation)
        val first = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        val firstManifest = manifest(first)
        firstManifest.items.map(_.itemId) shouldBe Vector("item-0", "item-1")
        firstManifest.items.size should be < definition.byId("tail").batch.maxItems
        firstManifest.sealedAt - durable(id).records(0L).acceptedAt should be < 1.hour.toMillis
        view(ref).inputClosed shouldBe false
        completed(first, success(firstManifest))
        release(calls, firstManifest.reservationId)
        createTestProbe[Unit]().awaitAssert({
          val current = view(ref)
          current.succeeded shouldBe 2
          current.activeBytes shouldBe 14L
        }, 5.seconds)
        ref ! next
        replies.expectMessage(BatchProtocol.Accepted(4L, "item-4", duplicate = false))
        closeInput(ref)
        Vector(Vector("item-2", "item-3"), Vector("item-4")).foreach { expected =>
          grant(calls.expectMessageType[ResourceProtocol.Reserve])
          val tail = deliveries.expectMessageType[DeliveryProtocol.Deliver]
          val m = manifest(tail)
          m.items.map(_.itemId) shouldBe expected
          completed(tail, success(m))
          release(calls, m.reservationId)
        }
        drained(ref, 5)
        durable(id).invalid shouldBe None
      } finally { testKit.stop(ref); clear(id) }
    }

    "give the second active batch a turn when slow journal acknowledgements consume every poll interval" in {
      val id = PersistenceId.ofUniqueId("batch-round-robin-" + UUID.randomUUID())
      val calls = createTestProbe[ResourceProtocol.Command]()
      val deliveries = createTestProbe[DeliveryProtocol.Command]()
      val writes = createTestProbe[ControlledJournal.Pending]()
      val definition = BatchDefinition("round-robin", "1", Vector(
        AreaDefinition("work", "1", BatchPolicy.single, Map("worker" -> 1), parallelism = 2)),
        settings.copy(maxConcurrentBatches = 2, pollIntervalMillis = 1))
      def coordinator: Behavior[BatchProtocol.Command] =
        BatchCoordinator.behavior(id, id.id, definition, calls.ref, _ => deliveries.ref, execution)
      var ref = spawn(coordinator)
      val firstId = id.id + "/batch-0"
      val secondId = id.id + "/batch-1"
      def advanced(batchId: String, phase: String): (ControlledJournal.Pending, ActiveBatch) = {
        val pending = writes.receiveMessage()
        pending.events.size shouldBe 1
        val batch = pending.events.head.payload.asInstanceOf[BatchEvent.Advanced].batch
        batch.manifest.batchId shouldBe batchId
        batch.phase shouldBe phase
        pending -> batch
      }
      def commit(batchId: String, phase: String, afterDue: Boolean = false): ActiveBatch = {
        val (pending, batch) = advanced(batchId, phase)
        if (afterDue) createTestProbe[Unit]().awaitAssert({
          System.currentTimeMillis() should be >= batch.nextDue
        }, 3.seconds, 1.millis)
        pending.release()
        batch
      }
      def queueFirst(expectedCalls: Int): Unit = {
        commit(firstId, "Reserving", afterDue = true).calls shouldBe expectedCalls
        val request = calls.expectMessageType[ResourceProtocol.Reserve]
        request.request.reservationId shouldBe firstId + "/resources"
        request.replyTo ! ResourceProtocol.Queued(request.request.reservationId)
        // The response's new due time must also expire before its ACK. Thus
        // firstId is ready at every selection; a head-only scheduler starves secondId.
        commit(firstId, "Reserving", afterDue = true).calls shouldBe expectedCalls
      }
      try {
        offer(ref, 0, "work")
        calls.expectMessageType[ResourceProtocol.Reserve].request.reservationId shouldBe firstId + "/resources"
        offer(ref, 1, "work")
        calls.expectMessageType[ResourceProtocol.Reserve].request.reservationId shouldBe secondId + "/resources"
        view(ref).batches.map(_.phase) shouldBe Vector("Reserving", "Reserving")
        // Recover genuine committed batches, dropping only the local outstanding
        // asks. Both are now ready together without constructing artificial events.
        testKit.stop(ref)
        val recovered = durable(id)
        recovered.invalid shouldBe None
        createTestProbe[Unit]().awaitAssert({
          System.currentTimeMillis() should be >= recovered.batches.valuesIterator.map(_.nextDue).max
        }, 3.seconds, 1.millis)
        ControlledJournal.probes.put(id.id, writes.ref)
        ref = spawn(coordinator)

        queueFirst(2)
        commit(secondId, "Reserving", afterDue = true).calls shouldBe 2
        val secondReservation = calls.expectMessageType[ResourceProtocol.Reserve]
        secondReservation.request.reservationId shouldBe secondId + "/resources"
        grant(secondReservation)
        commit(secondId, "Preparing")
        queueFirst(3)
        commit(secondId, "Delivering").delivery should not be empty
        queueFirst(4)
        val (intent, childCall) = advanced(secondId, "Delivering")
        childCall.calls shouldBe 3
        // No later writes need gating. Hold both peer replies instead, making
        // the final public view stable without a sleep or polling-budget expiry.
        ControlledJournal.probes.remove(id.id)
        intent.release()
        val child = deliveries.expectMessageType[DeliveryProtocol.Deliver]
        val m = manifest(child)
        m.batchId shouldBe secondId
        m.items.map(_.itemId) shouldBe Vector("item-1")
        calls.expectMessageType[ResourceProtocol.Reserve].request.reservationId shouldBe firstId + "/resources"
        val current = view(ref)
        current.batches.map(b => b.batchId -> b.phase).toMap shouldBe
          Map(firstId -> "Reserving", secondId -> "Delivering")
        current.active shouldBe 2
        current.succeeded shouldBe 0
        current.reason shouldBe None
        val committed = durable(id)
        committed.invalid shouldBe None
        committed.batches(secondId).delivery shouldBe Some(child.request)
        committed.batches(firstId).calls shouldBe 5
      } finally { testKit.stop(ref); clear(id) }
    }

    "seal all holders of a shared carried claim together rather than a ready or compatible subset" in {
      val cases = Vector(
        ("smaller-target", 1, "target", "same", 0L, false),
        ("different-key", 2, "target", "other", 0L, false),
        ("different-area", 2, "other", "same", 0L, false),
        ("not-yet-due", 2, "target", "same", 1.hour.toMillis, false),
        ("all-ready", 2, "target", "same", 0L, true))
      cases.foreach { case (name, maxItems, secondArea, secondKey, delay, canSeal) =>
        withClue(name + ": ") {
          val id = PersistenceId.ofUniqueId("batch-shared-" + name + "-" + UUID.randomUUID())
          val calls = createTestProbe[ResourceProtocol.Command]()
          val deliveries = createTestProbe[DeliveryProtocol.Command]()
          val drives = createTestProbe[BatchProtocol.Drive]()
          val replies = createTestProbe[BatchProtocol.Reply]()
          val definition = BatchDefinition("shared-claim", "1", Vector(
            AreaDefinition("target", "1", BatchPolicy.countOrWait(maxItems, 0), Map("worker" -> 1), parallelism = 2),
            AreaDefinition("other", "1", BatchPolicy.single, Map("worker" -> 1))),
            settings.copy(maxConcurrentBatches = 2))
          val claim = ResourceClaim(id.id + "/occupied", id.id + "/carrier", "claim-token", Map("port" -> 1))
          val records = Vector((0, "target", "same", 0L),
            (1, secondArea, secondKey, if (delay == 0L) 0L else System.currentTimeMillis() + delay)).map {
            case (n, area, key, due) =>
              val input = BatchProtocol.Offer(n.toLong, s"item-$n", ByteString("item"), area, key, replies.ref)
              ItemRecord(Item(input.sequence, input.itemId, input.payload.toVector, key, area, carried = Vector(claim)),
                input.fingerprint, "Pending", 0L, due)
          }
          // Seed a pending replay frontier to exercise defensive grouping in
          // isolation: the current completion API already rejects split routes/delays.
          val events = Vector[BatchEvent](BatchEvent.Initialized(id.id, definition.fingerprint)) ++
            records.map(BatchEvent.InputAccepted) :+ BatchEvent.InputClosed
          events.foreach(BatchCodec.eventToProto)
          ControlledJournal.storage.put(id.id, events.zipWithIndex.map { case (event, n) =>
            PersistentRepr(event, n.toLong + 1L, id.id)
          })
          val behavior = Behaviors.intercept(() => new BehaviorInterceptor[BatchProtocol.Command, BatchProtocol.Command] {
            override def aroundReceive(context: TypedActorContext[BatchProtocol.Command], message: BatchProtocol.Command,
                target: BehaviorInterceptor.ReceiveTarget[BatchProtocol.Command]): Behavior[BatchProtocol.Command] = {
              val next = target(context, message)
              message match { case drive: BatchProtocol.Drive => drives.ref ! drive; case _ => () }
              next
            }
          })(BatchCoordinator.behavior(id, id.id, definition, calls.ref, _ => deliveries.ref, execution))
          val ref = spawn(behavior)
          try {
            // Recovery has exactly one initial wake. This positive barrier proves
            // selection ran before checking the absence of a partial seal.
            drives.receiveMessage()
            if (canSeal) {
              grant(calls.expectMessageType[ResourceProtocol.Reserve])
              val child = deliveries.expectMessageType[DeliveryProtocol.Deliver]
              val m = manifest(child)
              m.items.map(_.itemId) shouldBe Vector("item-0", "item-1")
              m.items.foreach(_.carried shouldBe Vector(claim))
              view(ref).batches.map(_.itemIds) shouldBe Vector(Vector("item-0", "item-1"))
            } else {
              val current = view(ref)
              current.phase shouldBe "ClosingBlocked"
              current.active shouldBe 2
              current.batches shouldBe empty
              current.items.map(_.phase) shouldBe Vector("Pending", "Pending")
              calls.expectNoMessage(Duration.Zero)
              deliveries.expectNoMessage(Duration.Zero)
              durable(id).nextBatch shouldBe 0L
            }
            durable(id).invalid shouldBe None
          } finally { testKit.stop(ref); clear(id) }
        }
      }
    }
  }
}
