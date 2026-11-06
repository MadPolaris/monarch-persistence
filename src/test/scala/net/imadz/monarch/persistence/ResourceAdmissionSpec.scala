package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.persistence.typed.PersistenceId
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.resource._
import net.imadz.monarch.persistence.resource.ResourceProtocol._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration._

class ResourceAdmissionSpec extends ScalaTestWithActorTestKit(
    SerializedValidationJournal.config.withFallback(ConfigFactory.parseResources("resource-reference.conf")))
    with AnyWordSpecLike with Matchers {
  private val trusted = ByteString("authoritative ledger evidence")
  private val validating = new ResourceEvidenceValidator {
    override def validate(grant: Grant, proof: Proof): Either[String, Unit] =
      if (proof.evidence == trusted) Right(()) else Left("Unknown is not proof of physical release")
  }
  private def proof(grant: Grant): Proof = Proof(grant.ownerOperationKey, trusted)
  private def req(id: String, units: Map[String, Int] = Map("slot" -> 1)): Request = Request(id, s"operation-$id", units)

  private final class Fixture(
      val capacities: Map[String, Int] = Map("slot" -> 1),
      val settings: ResourceSettings = ResourceSettings(),
      val validator: ResourceEvidenceValidator = validating) {
    val id = PersistenceId.ofUniqueId(s"resource-${UUID.randomUUID()}")
    val writes = testKit.createTestProbe[SerializedValidationJournal.Pending]()
    val replies = testKit.createTestProbe[Reply]()
    SerializedValidationJournal.gates.put(id.id, writes.ref)
    var ref: ActorRef[Command] = testKit.spawn(ResourceCoordinator.behavior(id, capacities, validator, settings))
    val initial = writes.receiveMessage()

    def ready(): Unit = {
      initial.events.map(_.payload) shouldBe Vector(ResourceEvent.Defined(
        ResourceDefinition(id.id, capacities, settings), ResourceDefinition(id.id, capacities, settings).fingerprint))
      initial.release()
      query("not-recorded") shouldBe a[Unknown]
    }
    def query(id: String): Reply = { ref ! Query(id, replies.ref); replies.receiveMessage() }
    def send(command: ActorRef[Reply] => Command): Reply = { ref ! command(replies.ref); replies.receiveMessage() }
    def commit(): ResourceEvent = {
      val write = writes.receiveMessage()
      write.events.size shouldBe 1
      val event = write.events.head.payload.asInstanceOf[ResourceEvent]
      write.release(); event
    }
    def reserve(request: Request): Reply = {
      ref ! Reserve(request, replies.ref)
      commit()
      replies.receiveMessage()
    }
    def grant(request: Request): Grant = reserve(request) match {
      case Granted(g) => g
      case other => fail(s"expected a durable grant, got $other")
    }
    def release(grant: Grant): Unit = {
      ref ! Release(grant.reservationId, grant.token, proof(grant), replies.ref)
      commit() shouldBe ResourceEvent.ReleaseRecorded(grant.reservationId, grant.token, proof(grant))
      replies.expectMessage(Released(grant.reservationId))
    }
    def grantHead(): Grant = commit() match {
      case ResourceEvent.GrantRecorded(grant) =>
        query(grant.reservationId) shouldBe Granted(grant); grant
      case other => fail(s"expected FIFO grant, got $other")
    }
    def recreate(nextCapacities: Map[String, Int] = capacities, nextSettings: ResourceSettings = settings): Unit = {
      testKit.stop(ref)
      ref = testKit.spawn(ResourceCoordinator.behavior(id, nextCapacities, validator, nextSettings))
    }
    def close(): Unit = { testKit.stop(ref); SerializedValidationJournal.clear(id.id) }
  }

  "ResourceCoordinator" should {
    "commit the initial definition and grant before ACK or query visibility" in {
      val f = new Fixture()
      try {
        val request = req("first")
        val queries = testKit.createTestProbe[Reply]()
        f.ref ! Reserve(request, f.replies.ref)
        f.replies.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
        SerializedValidationJournal.storage.getOrElse(f.id.id, Vector.empty) shouldBe empty
        f.initial.release()
        val pending = f.writes.receiveMessage()
        pending.events.head.payload shouldBe a[ResourceEvent.GrantRecorded]
        f.ref ! Query(request.reservationId, queries.ref)
        f.replies.expectNoMessage(50.millis)
        queries.expectNoMessage(50.millis)
        SerializedValidationJournal.storage(f.id.id).size shouldBe 1
        pending.release()
        val granted = f.replies.expectMessageType[Granted]
        queries.expectMessage(granted)
        SerializedValidationJournal.storage(f.id.id).map(_.manifest) shouldBe Vector("RE1", "RE1")
      } finally f.close()
    }

    "keep two capacity-one contenders all-or-none and never turn Queued into launch permission" in {
      val f = new Fixture()
      try {
        f.ready()
        val first = f.grant(req("a"))
        f.reserve(req("b")) shouldBe Queued("b")
        (1 to 3).foreach(_ => f.query("b") shouldBe Queued("b"))
        f.query("a") shouldBe Granted(first)
        f.writes.expectNoMessage(50.millis) // Query does not perform a write or drive.
        f.ref ! Release(first.reservationId, first.token, proof(first), f.replies.ref)
        val release = f.writes.receiveMessage()
        release.events.head.payload shouldBe a[ResourceEvent.ReleaseRecorded]
        f.replies.expectNoMessage(50.millis)
        release.release()
        f.replies.expectMessage(Released("a"))
        val next = f.writes.receiveMessage()
        next.events.head.payload shouldBe a[ResourceEvent.GrantRecorded]
        val queries = testKit.createTestProbe[Reply]()
        f.ref ! Query("b", queries.ref)
        queries.expectNoMessage(50.millis)
        next.release()
        queries.expectMessageType[Granted].grant.reservationId shouldBe "b"
      } finally f.close()
    }

    "admit the oldest fitting bundle so a blocked intake cannot deadlock an occupant's continuation" in {
      val f = new Fixture(Map("execution" -> 1, "slot" -> 1))
      try {
        f.ready()
        val a = f.grant(req("a", Map("execution" -> 1)))
        f.reserve(req("b", Map("execution" -> 1, "slot" -> 1))) shouldBe Queued("b")
        val c = f.grant(req("c", Map("slot" -> 1)))
        f.query("b") shouldBe Queued("b")
        f.release(a)
        f.query("b") shouldBe Queued("b") // b never holds only half its bundle.
        f.release(c)
        val b = f.grantHead()
        b.reservationId shouldBe "b"
        b.units shouldBe Map("execution" -> 1, "slot" -> 1)
      } finally f.close()
    }

    "return the same grants after restart and reject changed requests even after release" in {
      val f = new Fixture()
      try {
        f.ready()
        val request = req("stable")
        val grant = f.grant(request)
        f.reserve(req("queued")) shouldBe Queued("queued")
        f.recreate()
        f.query("stable") shouldBe Granted(grant)
        f.send(Reserve(request, _)) shouldBe Granted(grant)
        f.query("queued") shouldBe Queued("queued")
        f.send(Reserve(request.copy(ownerOperationKey = "different"), _)) shouldBe a[Rejected]
        f.send(Reserve(req("queued").copy(ownerOperationKey = "different"), _)) shouldBe a[Rejected]
        f.release(grant)
        f.grantHead().reservationId shouldBe "queued"
        f.send(Release(grant.reservationId, grant.token, proof(grant), _)) shouldBe Released("stable")
        f.send(Release(grant.reservationId, grant.token, Proof(grant.ownerOperationKey, ByteString("different")), _)) shouldBe a[Rejected]
        f.send(Reserve(request, _)) shouldBe Released("stable")
        f.send(Reserve(request.copy(ownerOperationKey = "new-attempt"), _)) shouldBe a[Rejected]
        f.recreate()
        f.query("stable") shouldBe Released("stable")
        f.send(Reserve(request.copy(ownerOperationKey = "new-attempt"), _)) shouldBe a[Rejected]
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
    }

    "reject Unknown, untrusted and wrongly correlated proof without freeing any capacity" in {
      val calls = new AtomicInteger()
      val validator = new ResourceEvidenceValidator {
        override def validate(grant: Grant, proof: Proof): Either[String, Unit] = {
          calls.incrementAndGet(); validating.validate(grant, proof)
        }
      }
      val f = new Fixture(validator = validator)
      try {
        f.ready()
        val grant = f.grant(req("held"))
        f.reserve(req("waiting")) shouldBe Queued("waiting")
        f.send(Release(grant.reservationId, grant.token, Proof("other-owner", trusted), _)) shouldBe a[Rejected]
        calls.get() shouldBe 0 // Framework identity check precedes host validation.
        f.send(Release(grant.reservationId, "0" * 64, proof(grant), _)) shouldBe a[Rejected]
        calls.get() shouldBe 0
        f.send(Release(grant.reservationId, grant.token, Proof(grant.ownerOperationKey, ByteString("Unknown")), _)) shouldBe a[Rejected]
        calls.get() shouldBe 1
        f.query("held") shouldBe Granted(grant)
        f.query("waiting") shouldBe Queued("waiting")
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
      val defaults = new Fixture(validator = ResourceEvidenceValidator.reject)
      try {
        defaults.ready()
        val grant = defaults.grant(req("default"))
        defaults.send(Release(grant.reservationId, grant.token, proof(grant), _)) shouldBe a[Rejected]
        defaults.query("default") shouldBe Granted(grant)
      } finally defaults.close()
    }

    "durably partition a grant, retain destination occupancy and replay a lost transfer ACK identically" in {
      val f = new Fixture(Map("execution" -> 1, "slot" -> 1))
      try {
        f.ready()
        val original = f.grant(req("transport", Map("execution" -> 1, "slot" -> 1)))
        val lost = testKit.createTestProbe[Reply]()
        def transfer(to: ActorRef[Reply]): Transfer = Transfer(original.reservationId, original.token,
          "arrived", "occupancy-owner", Map("slot" -> 1), proof(original), to)
        f.ref ! transfer(lost.ref)
        val pending = f.writes.receiveMessage()
        pending.events.head.payload shouldBe a[ResourceEvent.TransferRecorded]
        lost.expectNoMessage(50.millis)
        pending.release()
        // Deliberately discard the sender's ACK. Query proves the transfer committed.
        val destination = f.query("arrived").asInstanceOf[Granted].grant
        f.query("transport") shouldBe a[Rejected]
        f.recreate()
        f.send(transfer) shouldBe Granted(destination)
        f.query("arrived") shouldBe Granted(destination)
        f.writes.expectNoMessage(50.millis)
        val execution = f.grant(req("next-execution", Map("execution" -> 1)))
        f.reserve(req("next-slot")) shouldBe Queued("next-slot")
        f.send(Release(original.reservationId, original.token, proof(original), _)) shouldBe a[Rejected]
        f.send(to => transfer(to).copy(toOwnerOperationKey = "changed")) shouldBe a[Rejected]
        f.send(to => transfer(to).copy(units = Map("execution" -> 1))) shouldBe a[Rejected]
        f.send(Transfer(execution.reservationId, execution.token, "arrived", "someone-else",
          execution.units, proof(execution), _)) shouldBe a[Rejected]
        f.query("next-slot") shouldBe Queued("next-slot")
        f.release(destination)
        f.grantHead().reservationId shouldBe "next-slot"
        f.send(transfer) shouldBe Granted(destination) // Immutable receipt, not current target status.
        f.query("arrived") shouldBe Released("arrived")
        f.recreate()
        f.send(transfer) shouldBe Granted(destination)
        f.query("arrived") shouldBe Released("arrived")
      } finally f.close()
    }

    "reject a transfer that adds units or reuses any destination identity" in {
      val f = new Fixture(Map("execution" -> 1, "slot" -> 2))
      try {
        f.ready()
        val grant = f.grant(req("source", Map("slot" -> 1)))
        f.send(Transfer(grant.reservationId, grant.token, "target", "owner", Map("slot" -> 2), proof(grant), _)) shouldBe a[Rejected]
        f.send(Transfer(grant.reservationId, grant.token, "target", "owner", Map("execution" -> 1), proof(grant), _)) shouldBe a[Rejected]
        f.send(Transfer(grant.reservationId, grant.token, grant.reservationId, "owner", Map("slot" -> 1), proof(grant), _)) shouldBe a[Rejected]
        f.query(grant.reservationId) shouldBe Granted(grant)
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
    }

    "recover a capacity or settings mismatch as queryable blocked, without grants or writes" in {
      val f = new Fixture()
      try {
        f.ready()
        val grant = f.grant(req("held"))
        f.reserve(req("waiting")) shouldBe Queued("waiting")
        f.recreate(nextCapacities = Map("slot" -> 2))
        f.query("held") shouldBe a[Unknown]
        f.query("waiting") shouldBe a[Unknown]
        f.send(Reserve(req("new"), _)) shouldBe a[Rejected]
        f.send(Release(grant.reservationId, grant.token, proof(grant), _)) shouldBe a[Rejected]
        f.writes.expectNoMessage(50.millis)
        f.recreate(nextSettings = ResourceSettings(maxQueued = 5))
        f.query("held") shouldBe a[Unknown]
        f.writes.expectNoMessage(50.millis)
        f.recreate()
        f.query("held") shouldBe Granted(grant)
        f.query("waiting") shouldBe Queued("waiting")
      } finally f.close()
    }

    "bound queued and lifetime records without forgetting released identities" in {
      val f = new Fixture(settings = ResourceSettings(maxReservations = 2, maxQueued = 1))
      try {
        f.ready()
        val first = f.grant(req("a"))
        f.reserve(req("b")) shouldBe Queued("b")
        f.send(Reserve(req("c"), _)) shouldBe a[Rejected]
        f.send(Transfer(first.reservationId, first.token, "target", "owner", first.units, proof(first), _)) shouldBe a[Rejected]
        f.release(first)
        val b = f.grantHead()
        f.release(b)
        f.send(Reserve(req("c"), _)) shouldBe a[Rejected]
        f.query("a") shouldBe Released("a")
        f.query("b") shouldBe Released("b")
        f.send(Reserve(req("a").copy(ownerOperationKey = "reuse"), _)) shouldBe a[Rejected]
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
      val noQueue = new Fixture(settings = ResourceSettings(maxQueued = 0))
      try {
        noQueue.ready(); noQueue.grant(req("a"))
        noQueue.send(Reserve(req("b"), _)) shouldBe a[Rejected]
      } finally noQueue.close()
    }

    "never overallocate under concurrent requests, including serialized snapshot recovery" in {
      val f = new Fixture()
      try {
        f.ready()
        SerializedValidationJournal.gates.remove(f.id.id)
        implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
        val count = 110 // Cross the automatic snapshot frontier as well as journal replay.
        (0 until count).foreach(i => Future { f.ref ! Reserve(req(s"concurrent-$i"), f.replies.ref) })
        val responses = f.replies.receiveMessages(count, 20.seconds)
        val grants = responses.collect { case Granted(g) => g }
        grants.size shouldBe 1
        responses.count(_.isInstanceOf[Queued]) shouldBe count - 1
        val views = (0 until count).map(i => f.query(s"concurrent-$i"))
        views.collect { case Granted(g) => g.units("slot") }.sum shouldBe 1
        f.replies.awaitAssert {
          ValidationSnapshotStore.snapshots.getOrElse(f.id.id, Vector.empty) should not be empty
        }
        f.recreate()
        f.query(grants.head.reservationId) shouldBe Granted(grants.head)
        val restored = (0 until count).map(i => f.query(s"concurrent-$i"))
        restored.count(_.isInstanceOf[Granted]) shouldBe 1
        restored.count(_.isInstanceOf[Queued]) shouldBe count - 1
      } finally f.close()
    }

    "return Unknown on a finite ask timeout without retrying or issuing an implicit release" in {
      val endpoint = testKit.createTestProbe[Command]()
      val result = testKit.createTestProbe[Reply]()
      val port = new ResourceAdmissionPort(endpoint.ref, 50.millis)(system.scheduler, system.executionContext)
      port.query("unavailable").foreach(result.ref ! _)(system.executionContext)
      endpoint.expectMessageType[Query].reservationId shouldBe "unavailable"
      result.expectMessageType[Unknown].reservationId shouldBe "unavailable"
      endpoint.expectNoMessage(100.millis)
    }
  }
}
