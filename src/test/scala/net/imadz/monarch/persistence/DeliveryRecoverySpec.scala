package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, ManualTime, ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, BehaviorInterceptor, SupervisorStrategy, TypedActorContext}
import akka.persistence.typed.PersistenceId
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.RunLease
import net.imadz.monarch.persistence.delivery._
import net.imadz.monarch.persistence.delivery.DeliveryProtocol._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Success

class DeliveryRecoverySpec extends ScalaTestWithActorTestKit(
    ManualTime.config.withFallback(SerializedValidationJournal.config)
      .withFallback(ConfigFactory.parseResources("delivery-reference.conf")))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: ExecutionContext = system.executionContext
  private val manual = ManualTime()(system)
  private val settings = DeliverySettings(queryTimeout = 10.minutes, pollInterval = 1.minute, maxPolls = 2)

  private final case class Invocation(kind: String, submission: RunSubmission, result: Promise[RunProtocol.Reply])
  private final class ProbePort(calls: ActorRef[Invocation]) extends RunSubmissionPort {
    private def call(kind: String, child: RunSubmission): Future[RunProtocol.Reply] = {
      val promise = Promise[RunProtocol.Reply]()
      calls ! Invocation(kind, child, promise)
      promise.future
    }
    override def ensure(child: RunSubmission): Future[RunProtocol.Reply] = call("ensure", child)
    override def query(child: RunSubmission): Future[RunProtocol.Reply] = call("query", child)
  }

  private def identity(child: RunSubmission): RunProtocol.Identity = RunProtocol.Identity(child.runId,
    child.definitionId, child.definitionVersion, child.codecVersion, ProtocolDigest.sha256(child.initialState),
    ProtocolDigest.fields(Vector(ByteString("child-step#0"))))

  private def childReply(child: RunSubmission, status: RunProtocol.Status = RunProtocol.Status.Running,
      bytes: ByteString = ByteString("committed"), reason: Option[String] = None): RunProtocol.Current =
    RunProtocol.Current(RunProtocol.View(Some(child.runId), status, Vector.empty, Some(bytes),
      Some(identity(child)), None, reason))

  private val notStarted = RunProtocol.Current(RunProtocol.View(None, RunProtocol.Status.NotStarted,
    Vector.empty, None, None, None, None))

  private final class Fixture(name: String, val policySettings: DeliverySettings = settings, restartable: Boolean = false) {
    val id: PersistenceId = PersistenceId.ofUniqueId(s"delivery-$name-${UUID.randomUUID()}")
    val request: Request = Request(id.id, "parent-run", "registered-target",
      RunSubmission(s"child-${id.id}", "child-definition", "1", "bytes-v1", ByteString("initial")), ByteString("payload"))
    val childIdentity: RunProtocol.Identity = identity(request.child)
    val now = new AtomicLong(1000000L)
    val calls: TestProbe[Invocation] = testKit.createTestProbe[Invocation]()
    val writes: TestProbe[SerializedValidationJournal.Pending] = testKit.createTestProbe[SerializedValidationJournal.Pending]()
    val replies: TestProbe[Reply] = testKit.createTestProbe[Reply]()
    val port = new ProbePort(calls.ref)
    SerializedValidationJournal.gates.put(id.id, writes.ref)

    private def underlying(nextSettings: DeliverySettings): Behavior[Command] = {
      val base = PersistentInbox.behaviorWithClock(id, request.deliveryId, port, nextSettings, () => now.get())
      if (!restartable) base
      else Behaviors.supervise(Behaviors.intercept(() => new BehaviorInterceptor[Command, Command] {
        override def aroundReceive(context: TypedActorContext[Command], message: Command,
            target: BehaviorInterceptor.ReceiveTarget[Command]): Behavior[Command] = message match {
          case Get("__restart-delivery-test", _) => throw new IllegalStateException("delivery restart test")
          case _ => target(context, message)
        }
      })(base)).onFailure[IllegalStateException](SupervisorStrategy.restart)
    }

    var ref: ActorRef[Command] = testKit.spawn(underlying(policySettings))
    def respawn(nextSettings: DeliverySettings = policySettings): Unit = { ref = testKit.spawn(underlying(nextSettings)) }
    def recreate(nextSettings: DeliverySettings = policySettings): Unit = { testKit.stop(ref); respawn(nextSettings) }
    def commit(expected: DeliveryEvent): Unit = {
      val pending = writes.receiveMessage()
      pending.events.map(_.payload) shouldBe Vector(expected)
      pending.release()
    }
    def commitBlocked(): String = {
      val pending = writes.receiveMessage()
      val reason = pending.events.head.payload match {
        case DeliveryEvent.Blocked(message) => message
        case other => fail(s"expected Blocked, got $other")
      }
      pending.release()
      reason
    }
    def current(): View = {
      ref ! Get(request.deliveryId, replies.ref)
      replies.receiveMessage() match {
        case Current(view) => view
        case other => fail(s"expected Current, got $other")
      }
    }
    def start(): Invocation = {
      ref ! Deliver(request, replies.ref)
      commit(DeliveryEvent.Accepted(request, request.fingerprint, policySettings.policy))
      replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Accepted
      startCall(1)
    }
    def startCall(attempt: Int): Invocation = {
      commit(DeliveryEvent.ChildStartIntent(attempt))
      val call = calls.receiveMessage()
      call.kind shouldBe "ensure"
      call.submission shouldBe request.child
      current().phase shouldBe Phase.Starting // Barrier: the I/O watchdog is installed.
      call
    }
    def running(call: Invocation): Unit = {
      call.result.success(childReply(request.child))
      commit(DeliveryEvent.ChildRunning(childIdentity, Some(now.get() + policySettings.pollInterval.toMillis)))
      val view = current()
      view.phase shouldBe Phase.Running
      view.result shouldBe None
    }
    def advance(by: FiniteDuration = policySettings.pollInterval): Unit = {
      now.addAndGet(by.toMillis)
      manual.timePasses(by)
    }
    def query(poll: Int): Invocation = {
      commit(DeliveryEvent.ChildQueryIntent(poll))
      val call = calls.receiveMessage()
      call.kind shouldBe "query"
      call.submission shouldBe request.child
      current() // Barrier: watchdog is installed; query does not assert completion.
      call
    }
    def reconcile(round: Int, poll: Int): Invocation = {
      ref ! Reconcile(request.deliveryId, replies.ref)
      commit(DeliveryEvent.Reconciled(round, now.get()))
      val granted = replies.receiveMessage().asInstanceOf[Current].view
      granted.phase shouldBe Phase.Blocked
      granted.reconciling shouldBe true
      query(poll)
    }
    def complete(call: Invocation, bytes: ByteString = ByteString("done")): Unit = {
      call.result.success(childReply(request.child, RunProtocol.Status.Completed, bytes))
      commit(DeliveryEvent.ChildTerminalRecorded(childIdentity, Phase.Completed, bytes, None))
      current().result shouldBe Some(bytes)
    }
    def close(): Unit = { testKit.stop(ref); SerializedValidationJournal.clear(id.id) }
  }

  "PersistentInbox" should {
    "commit acceptance before ACK, child intent before ensure, and terminal before result visibility" in {
      val config = ConfigFactory.parseString("""
        akka.actor.allow-java-serialization = off
        akka.persistence.journal.plugin = "controlled-journal"
        controlled-journal.class = "net.imadz.monarch.persistence.ControlledJournal"
        controlled-journal.plugin-dispatcher = "akka.actor.default-dispatcher"
        akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
      """).withFallback(ConfigFactory.parseResources("delivery-reference.conf"))
      val kit = ActorTestKit(s"delivery-controlled-${UUID.randomUUID()}", config)
      val id = PersistenceId.ofUniqueId(s"delivery-controlled-${UUID.randomUUID()}")
      val writes = kit.createTestProbe[ControlledJournal.Pending]()
      val calls = kit.createTestProbe[Invocation]()
      val replies = kit.createTestProbe[Reply]()
      ControlledJournal.probes.put(id.id, writes.ref)
      try {
        val request = Request(id.id, "parent", "target", RunSubmission("child", "definition", "1", "1", ByteString("initial")),
          ByteString("payload"))
        val ref = kit.spawn(PersistentInbox.behavior(id, id.id, new ProbePort(calls.ref))(kit.system.executionContext))
        ref ! Deliver(request, replies.ref)
        val acceptance = writes.receiveMessage()
        acceptance.events.head.payload shouldBe a[DeliveryEvent.Accepted]
        calls.expectNoMessage(50.millis)
        replies.expectNoMessage(50.millis)
        acceptance.release()
        replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Accepted
        val intent = writes.receiveMessage()
        intent.events.head.payload shouldBe DeliveryEvent.ChildStartIntent(1)
        calls.expectNoMessage(50.millis)
        intent.release()
        val call = calls.receiveMessage()
        call.kind shouldBe "ensure"
        call.submission shouldBe request.child
        val bytes = ByteString(0, -1, 42)
        call.result.success(childReply(request.child, RunProtocol.Status.Completed, bytes))
        val terminal = writes.receiveMessage()
        terminal.events.head.payload shouldBe DeliveryEvent.ChildTerminalRecorded(identity(request.child), Phase.Completed, bytes, None)
        ref ! Get(id.id, replies.ref)
        replies.expectNoMessage(50.millis)
        terminal.release()
        replies.receiveMessage().asInstanceOf[Current].view.result shouldBe Some(bytes)
        ref ! Deliver(request, replies.ref)
        replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Completed
        Vector(request.copy(payload = ByteString("changed")), request.copy(target = "other"),
          request.copy(sourceRunId = "other-parent"), request.copy(child = request.child.copy(runId = "other-child")),
          request.copy(child = request.child.copy(initialState = ByteString("changed")))).foreach { conflict =>
          ref ! Deliver(conflict, replies.ref)
          replies.receiveMessage().asInstanceOf[Rejected].reason should include("different immutable request")
        }
        writes.expectNoMessage(50.millis)
        calls.expectNoMessage(50.millis)
        kit.stop(ref)
      } finally {
        kit.shutdownTestKit()
        ControlledJournal.probes.remove(id.id)
        ControlledJournal.storage.remove(id.id)
      }
    }

    "recover a lost acceptance ACK and an uncommitted child intent without orphaning or changing the child" in {
      val f = new Fixture("acceptance-ack-lost")
      val discard = testKit.spawn(Behaviors.ignore[Reply])
      try {
        f.ref ! Deliver(f.request, discard)
        f.commit(DeliveryEvent.Accepted(f.request, f.request.fingerprint, settings.policy))
        val uncommitted = f.writes.receiveMessage()
        uncommitted.events.head.payload shouldBe DeliveryEvent.ChildStartIntent(1)
        f.calls.expectNoMessage(50.millis)
        testKit.stop(f.ref)
        uncommitted.fail(new IllegalStateException("child intent never committed"))
        f.respawn()
        val call = f.startCall(1)
        f.ref ! Deliver(f.request, f.replies.ref)
        val duplicate = f.replies.receiveMessage().asInstanceOf[Current].view
        duplicate.childRunId shouldBe Some(f.request.child.runId)
        duplicate.requestDigest shouldBe Some(f.request.fingerprint)
        duplicate.phase shouldBe Phase.Starting
        f.complete(call)
        f.calls.expectNoMessage(50.millis)
        SerializedValidationJournal.storage(f.id.id).map(_.serializerId).toSet shouldBe Set(77102150)
      } finally { testKit.stop(discard); f.close() }
    }

    "re-ensure the frozen submission after a lost start ACK and ignore the old owner's late failure" in {
      val f = new Fixture("start-ack-lost")
      try {
        val lost = f.start()
        f.recreate()
        val resumed = f.startCall(2)
        resumed.submission shouldBe lost.submission
        resumed.submission.fingerprint shouldBe lost.submission.fingerprint
        f.complete(resumed, ByteString("existing-child-result"))
        lost.result.success(childReply(f.request.child, RunProtocol.Status.Failed, ByteString("late"), Some("old owner")))
        f.current().phase shouldBe Phase.Completed
        f.current().result shouldBe Some(ByteString("existing-child-result"))
        f.calls.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
    }

    "recover Running by query and record completion without any child-done notification" in {
      val f = new Fixture("missed-notification")
      try {
        f.running(f.start())
        f.recreate()
        f.current().phase shouldBe Phase.Running
        f.calls.expectNoMessage(50.millis)
        f.advance()
        val query = f.query(1)
        query.result.success(childReply(f.request.child, RunProtocol.Status.Completed, ByteString("committed-result")))
        val terminal = f.writes.receiveMessage()
        terminal.events.head.payload shouldBe DeliveryEvent.ChildTerminalRecorded(f.childIdentity,
          Phase.Completed, ByteString("committed-result"), None)
        f.ref ! Get(f.request.deliveryId, f.replies.ref)
        f.replies.expectNoMessage(50.millis)
        terminal.release()
        f.replies.receiveMessage().asInstanceOf[Current].view.result shouldBe Some(ByteString("committed-result"))
        f.recreate()
        f.current().phase shouldBe Phase.Completed
        f.ref ! Reconcile(f.request.deliveryId, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Completed
        f.advance(1.day)
        f.calls.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
    }

    "treat a queried NotStarted as missing work and only re-ensure the exact already-acknowledged child" in {
      val f = new Fixture("query-not-started")
      try {
        f.running(f.start())
        f.advance()
        f.query(1).result.success(notStarted)
        val pending = f.writes.receiveMessage()
        pending.events.head.payload shouldBe DeliveryEvent.ChildStartIntent(2)
        f.calls.expectNoMessage(50.millis)
        pending.release()
        val ensured = f.calls.receiveMessage()
        ensured.kind shouldBe "ensure"
        ensured.submission shouldBe f.request.child
        f.current().phase shouldBe Phase.Starting
        f.current().result shouldBe None
        f.complete(ensured)
      } finally f.close()
    }

    "reject foreign identity, changing plan, missing committed state and over-limit terminal bytes" in {
      val corruptions: Vector[(String, RunProtocol.View => RunProtocol.View)] = Vector(
        "view-id" -> ((v: RunProtocol.View) => v.copy(runId = Some("other-run"))),
        "identity-id" -> ((v: RunProtocol.View) => v.copy(identity = v.identity.map(_.copy(runId = "other-run")))),
        "definition" -> ((v: RunProtocol.View) => v.copy(identity = v.identity.map(_.copy(definitionId = "other")))),
        "definition-version" -> ((v: RunProtocol.View) => v.copy(identity = v.identity.map(_.copy(definitionVersion = "2")))),
        "codec-version" -> ((v: RunProtocol.View) => v.copy(identity = v.identity.map(_.copy(codecVersion = "2")))),
        "initial-state" -> ((v: RunProtocol.View) => v.copy(identity = v.identity.map(_.copy(initialDigest = "0" * 64)))),
        "plan" -> ((v: RunProtocol.View) => v.copy(identity = v.identity.map(_.copy(planDigest = "0" * 64)))),
        "identity-absent" -> ((v: RunProtocol.View) => v.copy(identity = None)),
        "state-absent" -> ((v: RunProtocol.View) => v.copy(lastState = None)),
        "state-size" -> ((v: RunProtocol.View) => v.copy(lastState = Some(ByteString(new Array[Byte](17))))),
        "result-size" -> ((v: RunProtocol.View) => v.copy(lastState = Some(ByteString(new Array[Byte](9))))),
        "status" -> ((v: RunProtocol.View) => v.copy(status = null)))
      corruptions.foreach { case (name, corrupt) =>
        val f = new Fixture(s"invalid-$name", settings.copy(limits = ProtocolLimits(maxStateBytes = 16, maxResultBytes = 8)))
        try {
          f.running(f.start())
          f.advance()
          val query = f.query(1)
          query.result.success(RunProtocol.Current(corrupt(childReply(f.request.child, RunProtocol.Status.Completed,
            ByteString("done")).view)))
          f.commitBlocked()
          f.current().phase shouldBe Phase.Blocked
          f.current().result shouldBe None
          f.complete(f.reconcile(1, 2)) // Invalid evidence never replaces the first observed plan identity.
        } finally f.close()
      }
    }

    "persist unknown, unavailable, recovery-blocked, rejection and failed asks as Blocked rather than Failed" in {
      Vector("unavailable", "recovery-blocked", "rejected", "ask-timeout", "not-started", "null").foreach { kind =>
        val f = new Fixture(s"unknown-$kind")
        try {
          val call = f.start()
          kind match {
            case "unavailable" => call.result.success(RunProtocol.Unavailable(Some(f.request.child.runId), "unknown"))
            case "recovery-blocked" => call.result.success(childReply(f.request.child,
              RunProtocol.Status.RecoveryBlocked, reason = Some("unsupported definition version")))
            case "rejected" => call.result.success(RunProtocol.Rejected(f.request.child.runId, "conflicting start request"))
            case "ask-timeout" => call.result.failure(new TimeoutException("no child ACK"))
            case "not-started" => call.result.success(notStarted)
            case "null" => call.result.success(null)
          }
          f.commitBlocked()
          f.current().phase shouldBe Phase.Blocked
          f.current().result shouldBe None
          f.advance(1.hour)
          f.calls.expectNoMessage(50.millis)
          f.writes.expectNoMessage(50.millis)
          val reconciliation = f.reconcile(1, 1)
          reconciliation.kind shouldBe "query"
          f.complete(reconciliation)
        } finally f.close()
      }
    }

    "freeze finite automatic and operator budgets so new settings or duplicate reconciliation cannot replenish them" in {
      val f = new Fixture("finite-budget", settings.copy(maxPolls = 1))
      try {
        f.running(f.start())
        f.advance()
        f.query(1).result.success(childReply(f.request.child))
        f.commit(DeliveryEvent.ChildRunning(f.childIdentity, None))
        f.current().phase shouldBe Phase.Blocked
        f.advance(1.hour)
        f.calls.expectNoMessage(50.millis)
        f.recreate(settings.copy(maxPolls = 1000, queryTimeout = 1.millis, pollInterval = 1.millis))
        f.current().phase shouldBe Phase.Blocked
        (1 to DeliverySettings.MaxReconciliations).foreach { round =>
          val query = f.reconcile(round, round + 1)
          f.ref ! Reconcile(f.request.deliveryId, f.replies.ref)
          f.replies.receiveMessage().asInstanceOf[Rejected].reason should include("still outstanding")
          query.result.success(childReply(f.request.child))
          f.commit(DeliveryEvent.ChildRunning(f.childIdentity, None))
          f.current().phase shouldBe Phase.Blocked
          f.advance(10.seconds)
          f.writes.expectNoMessage(50.millis)
          f.calls.expectNoMessage(50.millis)
        }
        f.ref ! Reconcile(f.request.deliveryId, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Rejected].reason should include("budget exhausted")
        f.writes.expectNoMessage(50.millis)
        f.calls.expectNoMessage(50.millis)
      } finally f.close()
    }

    "retain one real outstanding call after timeout and accept its verified late committed result" in {
      val f = new Fixture("outstanding", settings.copy(queryTimeout = 1.second))
      try {
        val call = f.start()
        f.advance(1.second)
        f.commitBlocked() should include("original call remains outstanding")
        f.current().phase shouldBe Phase.Blocked
        (1 to 4).foreach { _ =>
          f.ref ! Reconcile(f.request.deliveryId, f.replies.ref)
          f.replies.receiveMessage().asInstanceOf[Rejected].reason should include("still outstanding")
          f.ref ! Deliver(f.request, f.replies.ref)
          f.replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Blocked
        }
        f.advance(1.day)
        f.writes.expectNoMessage(50.millis)
        f.calls.expectNoMessage(50.millis)
        f.complete(call, ByteString("late-committed-result"))
      } finally f.close()
    }

    "run watchdogs on the actor dispatcher even when the supplied I/O executor blocks before returning a Future" in {
      val kit = ActorTestKit(s"delivery-blocked-io-${UUID.randomUUID()}",
        SerializedValidationJournal.config.withFallback(ConfigFactory.parseResources("delivery-reference.conf")))
      val io = ExecutionContext.fromExecutorService(java.util.concurrent.Executors.newSingleThreadExecutor())
      val entered = new java.util.concurrent.CountDownLatch(1)
      val release = new java.util.concurrent.CountDownLatch(1)
      val id = PersistenceId.ofUniqueId(s"delivery-blocked-io-${UUID.randomUUID()}")
      val request = Request(id.id, "parent", "target", RunSubmission("child", "definition", "1", "1", ByteString.empty), ByteString.empty)
      val port = new RunSubmissionPort {
        override def ensure(child: RunSubmission): Future[RunProtocol.Reply] = {
          entered.countDown()
          release.await()
          Future.successful(RunProtocol.Unavailable(Some(child.runId), "unknown"))
        }
        override def query(child: RunSubmission): Future[RunProtocol.Reply] =
          Future.successful(RunProtocol.Unavailable(Some(child.runId), "unknown"))
      }
      try {
        val ref = kit.spawn(PersistentInbox.behavior(id, request.deliveryId, port,
          DeliverySettings(queryTimeout = 150.millis, pollInterval = 10.seconds))(io))
        val replies = kit.createTestProbe[Reply]()
        ref ! Deliver(request, replies.ref)
        replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Accepted
        entered.await(3, java.util.concurrent.TimeUnit.SECONDS) shouldBe true
        replies.awaitAssert({
          ref ! Get(request.deliveryId, replies.ref)
          replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Blocked
        }, 3.seconds)
        release.getCount shouldBe 1L
        ref ! Reconcile(request.deliveryId, replies.ref)
        replies.receiveMessage().asInstanceOf[Rejected].reason should include("still outstanding")
        kit.stop(ref)
      } finally {
        release.countDown()
        io.shutdownNow()
        kit.shutdownTestKit()
        SerializedValidationJournal.clear(id.id)
      }
    }

    "fence callbacks across a same-ref restart and cancel a revoked owner's timers" in {
      val f = new Fixture("same-ref", restartable = true)
      try {
        val original = f.start()
        f.ref ! Get("__restart-delivery-test", f.replies.ref)
        f.ref ! CallFinished(-1L, 1L, Success(childReply(f.request.child, RunProtocol.Status.Completed)))
        f.ref ! CallTimedOut(-1L, 1L)
        val resumed = f.startCall(2)
        original.result.success(childReply(f.request.child, RunProtocol.Status.Completed, ByteString("old-owner")))
        f.current().phase shouldBe Phase.Starting
        f.current().result shouldBe None
        f.writes.expectNoMessage(50.millis)
        f.complete(resumed, ByteString("new-owner"))
      } finally f.close()

      val revoked = new Fixture("revoked")
      var replacement: Option[RunLease] = None
      try {
        val original = revoked.start()
        replacement = Some(RunLease.acquire(PersistentInbox.leaseKey(revoked.id), () => ()))
        original.result.success(childReply(revoked.request.child, RunProtocol.Status.Completed))
        revoked.current().phase shouldBe Phase.RecoveryBlocked
        revoked.current().result shouldBe None
        revoked.ref ! Deliver(revoked.request.copy(target = "conflict"), revoked.replies.ref)
        revoked.replies.receiveMessage() shouldBe a[Rejected]
        revoked.advance(1.day)
        revoked.calls.expectNoMessage(50.millis)
        revoked.writes.expectNoMessage(50.millis)
      } finally { replacement.foreach(_.revoke()); revoked.close() }
    }

    "retain a true committed child failure monotonically without another start or query" in {
      val f = new Fixture("child-failed")
      try {
        val call = f.start()
        val bytes = ByteString("last-committed-checkpoint")
        call.result.success(childReply(f.request.child, RunProtocol.Status.Failed, bytes, Some("business failure")))
        f.commit(DeliveryEvent.ChildTerminalRecorded(f.childIdentity, Phase.Failed, bytes, Some("business failure")))
        f.current().phase shouldBe Phase.Failed
        f.current().result shouldBe Some(bytes)
        f.recreate()
        f.ref ! Deliver(f.request, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Failed
        f.ref ! Reconcile(f.request.deliveryId, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Failed
        f.advance(1.day)
        f.calls.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
    }
  }
}
