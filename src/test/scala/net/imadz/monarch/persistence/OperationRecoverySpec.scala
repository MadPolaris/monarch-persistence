package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, ManualTime, ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, BehaviorInterceptor, SupervisorStrategy, TypedActorContext}
import akka.persistence.typed.PersistenceId
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.RunLease
import net.imadz.monarch.persistence.operation._
import net.imadz.monarch.persistence.operation.OperationProtocol._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Success

class OperationRecoverySpec extends ScalaTestWithActorTestKit(
    ManualTime.config.withFallback(SerializedValidationJournal.config)
      .withFallback(ConfigFactory.parseResources("operation-reference.conf")))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: ExecutionContext = system.executionContext
  private val manual = ManualTime()(system)
  private val proofBytes = ByteString("durable connector proof")
  private val settings = OperationSettings(ioTimeout = 10.minutes, checkInterval = 1.minute,
    maxChecks = 2, manualCheckBudget = 1, maxReconciliations = 1)

  private final case class Invocation(kind: String, request: Request, previous: Option[Evidence],
      result: Promise[Observation])

  private final class ProbePort(calls: ActorRef[Invocation],
      override val connectorVersion: String = "1", trustedProof: Boolean = true,
      authorizeResubmission: Boolean = false) extends ExternalOperationPort {
    override val connectorId: String = "fixture"
    private def call(kind: String, request: Request, previous: Option[Evidence]): Future[Observation] = {
      val result = Promise[Observation]()
      calls ! Invocation(kind, request, previous, result)
      result.future
    }
    override def submit(request: Request): Future[Observation] = call("submit", request, None)
    override def query(request: Request, previous: Option[Evidence]): Future[Observation] = call("query", request, previous)
    override def validateEvidence(request: Request, observation: WithEvidence): Either[String, Unit] =
      if (!trustedProof) super.validateEvidence(request, observation)
      else if (observation.evidence.payload == proofBytes) Right(())
      else Left("missing connector ledger proof")
    override def validateSafeToSubmit(request: Request, proof: Evidence,
        previous: Option[Evidence]): Either[String, Unit] =
      if (authorizeResubmission) Right(()) else super.validateSafeToSubmit(request, proof, previous)
  }

  private def evidence(request: Request): Evidence = Evidence(request.operationKey, request.fingerprint,
    request.ledgerEpoch, Some("task-1"), proofBytes)

  private final class Fixture(name: String, val policySettings: OperationSettings = settings,
      trustedProof: Boolean = true, authorizeResubmission: Boolean = false, restartable: Boolean = false) {
    val id: PersistenceId = PersistenceId.ofUniqueId(s"operation-$name-${UUID.randomUUID()}")
    val request: Request = Request(id.id, s"effect-$name", "step#0", "fixture", "1", "durable-ledger-A",
      ByteString("rq"), Some("reservation-1"))
    val now = new AtomicLong(1000000L)
    val calls: TestProbe[Invocation] = testKit.createTestProbe[Invocation]()
    val writes: TestProbe[SerializedValidationJournal.Pending] = testKit.createTestProbe[SerializedValidationJournal.Pending]()
    val replies: TestProbe[Reply] = testKit.createTestProbe[Reply]()
    val port = new ProbePort(calls.ref, trustedProof = trustedProof, authorizeResubmission = authorizeResubmission)
    SerializedValidationJournal.gates.put(id.id, writes.ref)

    private def underlying(nextPort: ExternalOperationPort, nextSettings: OperationSettings): Behavior[Command] = {
      val base = PersistentOperation.behaviorWithClock(id, request.operationKey, nextPort, nextSettings, () => now.get())
      if (!restartable) base
      else {
        val intercepted = Behaviors.intercept(() => new BehaviorInterceptor[Command, Command] {
          override def aroundReceive(context: TypedActorContext[Command], message: Command,
              target: BehaviorInterceptor.ReceiveTarget[Command]): Behavior[Command] = message match {
            case Get("__restart-operation-test", _) => throw new IllegalStateException("operation restart test")
            case _ => target(context, message)
          }
        })(base)
        Behaviors.supervise(intercepted).onFailure[IllegalStateException](SupervisorStrategy.restart)
      }
    }
    var ref: ActorRef[Command] = testKit.spawn(underlying(port, policySettings))

    def recreate(nextPort: ExternalOperationPort = port, nextSettings: OperationSettings = policySettings): Unit = {
      testKit.stop(ref)
      ref = testKit.spawn(underlying(nextPort, nextSettings))
    }
    def commit(expected: OperationEvent): Unit = {
      val pending = writes.receiveMessage()
      pending.events.map(_.payload) shouldBe Vector(expected)
      pending.release()
    }
    def commitBlocked(): String = {
      val pending = writes.receiveMessage()
      val reason = pending.events.head.payload match {
        case OperationEvent.Blocked(message) => message
        case other => fail(s"expected Blocked, got $other")
      }
      pending.release()
      reason
    }
    def current(): View = {
      ref ! Get(request.operationKey, replies.ref)
      replies.receiveMessage() match {
        case Current(view) => view
        case other => fail(s"expected Current, got $other")
      }
    }
    def start(): Invocation = {
      ref ! Ensure(request, replies.ref)
      commit(OperationEvent.Prepared(request, request.fingerprint, policySettings.policy))
      replies.receiveMessage() match {
        case Current(view) => view.phase shouldBe Phase.Prepared
        case other => fail(s"expected prepared acknowledgement, got $other")
      }
      commit(OperationEvent.DispatchStarted(1, None))
      val call = calls.receiveMessage()
      call.kind shouldBe "submit"
      call.request shouldBe request
      current().phase shouldBe Phase.Dispatching // Barrier: the I/O timer is now installed.
      call
    }
    def advance(by: FiniteDuration = policySettings.checkInterval): Unit = {
      now.addAndGet(by.toMillis)
      manual.timePasses(by)
    }
    def query(number: Int): Invocation = {
      commit(OperationEvent.QueryStarted(number))
      val call = calls.receiveMessage()
      call.kind shouldBe "query"
      call.request shouldBe request
      call
    }
    def scheduledUnknown(reason: String): Unit =
      commit(OperationEvent.UnknownObserved(reason, Some(now.get() + policySettings.checkInterval.toMillis)))
    def close(): Unit = {
      testKit.stop(ref)
      SerializedValidationJournal.clear(id.id)
    }
  }

  "PersistentOperation" should {
    "gate intent ACK, dispatch and terminal visibility on real journal acknowledgements" in {
      val config = ConfigFactory.parseString("""
        akka.actor.allow-java-serialization = off
        akka.persistence.journal.plugin = "controlled-journal"
        controlled-journal.class = "net.imadz.monarch.persistence.ControlledJournal"
        controlled-journal.plugin-dispatcher = "akka.actor.default-dispatcher"
        akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
      """).withFallback(ConfigFactory.parseResources("operation-reference.conf"))
      val kit = ActorTestKit(s"operation-controlled-${UUID.randomUUID()}", config)
      val id = PersistenceId.ofUniqueId(s"operation-controlled-${UUID.randomUUID()}")
      val writes = kit.createTestProbe[ControlledJournal.Pending]()
      val calls = kit.createTestProbe[Invocation]()
      val replies = kit.createTestProbe[Reply]()
      ControlledJournal.probes.put(id.id, writes.ref)
      try {
        val request = Request(id.id, "effect", "step#0", "fixture", "1", "durable-ledger-A", ByteString("request"))
        val ref = kit.spawn(PersistentOperation.behavior(id, id.id, new ProbePort(calls.ref))(kit.system.executionContext))
        ref ! Ensure(request, replies.ref)
        val prepared = writes.receiveMessage()
        prepared.events.head.payload shouldBe a[OperationEvent.Prepared]
        calls.expectNoMessage(50.millis)
        replies.expectNoMessage(50.millis)
        prepared.release()
        replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Prepared
        val dispatch = writes.receiveMessage()
        dispatch.events.head.payload shouldBe OperationEvent.DispatchStarted(1, None)
        calls.expectNoMessage(50.millis)
        dispatch.release()
        val submission = calls.receiveMessage()
        submission.request shouldBe request
        val terminal = Succeeded(ByteString(0, -1, 42), evidence(request))
        submission.result.success(Confirmed(terminal))
        val committedResult = writes.receiveMessage()
        committedResult.events.head.payload shouldBe OperationEvent.TerminalRecorded(terminal)
        ref ! Get(id.id, replies.ref)
        replies.expectNoMessage(50.millis)
        committedResult.release()
        replies.receiveMessage().asInstanceOf[Current].view.terminal shouldBe Some(terminal)
        ref ! Ensure(request, replies.ref)
        replies.receiveMessage().asInstanceOf[Current].view.terminal shouldBe Some(terminal)
        calls.expectNoMessage(50.millis)
        kit.stop(ref)
      } finally {
        kit.shutdownTestKit()
        ControlledJournal.probes.remove(id.id)
        ControlledJournal.storage.remove(id.id)
      }
    }

    "deliver the timeout watchdog while a dedicated single I/O worker is blocked" in {
      val kit = ActorTestKit(s"operation-blocking-io-${UUID.randomUUID()}",
        SerializedValidationJournal.config.withFallback(ConfigFactory.parseResources("operation-reference.conf")))
      val executor = java.util.concurrent.Executors.newSingleThreadExecutor()
      val io = ExecutionContext.fromExecutorService(executor)
      val entered = new java.util.concurrent.CountDownLatch(1)
      val release = new java.util.concurrent.CountDownLatch(1)
      val id = PersistenceId.ofUniqueId("blocking-io-" + UUID.randomUUID())
      val req = Request(id.id, "effect", "submit", "blocking", "1", "ledger-1", ByteString("request"))
      val port = new ExternalOperationPort {
        override val connectorId = "blocking"
        override val connectorVersion = "1"
        override def submit(request: Request): Future[Observation] = Future {
          entered.countDown()
          release.await()
          Unknown("unblocked without authoritative result")
        }(io)
        override def query(request: Request, previous: Option[Evidence]): Future[Observation] =
          Future.successful(Unknown("query"))
      }
      try {
        val ref = kit.spawn(PersistentOperation.behavior(id, req.operationKey, port,
          OperationSettings(ioTimeout = 150.millis, checkInterval = 10.seconds))(io))
        val reply = kit.createTestProbe[Reply]()
        ref ! Ensure(req, reply.ref)
        reply.receiveMessage() shouldBe a[Current]
        entered.await(3, java.util.concurrent.TimeUnit.SECONDS) shouldBe true
        reply.awaitAssert({
          ref ! Get(req.operationKey, reply.ref)
          reply.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Blocked
        }, 3.seconds)
        release.getCount shouldBe 1L // Watchdog ran without freeing the blocked I/O executor.
        ref ! Reconcile(req.operationKey, reply.ref)
        reply.receiveMessage() shouldBe a[Rejected]
        kit.stop(ref)
      } finally {
        release.countDown()
        io.shutdownNow()
        kit.shutdownTestKit()
        SerializedValidationJournal.clear(id.id)
      }
    }

    "recover an accepted-reply-lost Dispatching by querying the original request without resubmission" in {
      val f = new Fixture("lost-acceptance")
      try {
        val lost = f.start()
        f.current().phase shouldBe Phase.Dispatching
        f.recreate()
        val query = f.query(1)
        query.previous shouldBe None // No task receipt was available before the crash.
        query.request.operationKey shouldBe lost.request.operationKey
        query.request.fingerprint shouldBe lost.request.fingerprint
        val terminal = Succeeded(ByteString("authoritative result"), evidence(f.request))
        query.result.success(Confirmed(terminal))
        f.commit(OperationEvent.TerminalRecorded(terminal))
        f.current().terminal shouldBe Some(terminal)
        lost.result.success(Confirmed(KnownFailed("LATE", "old owner", ByteString.empty, evidence(f.request))))
        f.current().terminal shouldBe Some(terminal)
        f.calls.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
        SerializedValidationJournal.storage(f.id.id).map(_.serializerId).toSet shouldBe Set(77102130)
      } finally f.close()
    }

    "recover a committed Prepared intent and dispatch only after the new dispatch record commits" in {
      val f = new Fixture("prepared")
      try {
        f.ref ! Ensure(f.request, f.replies.ref)
        f.commit(OperationEvent.Prepared(f.request, f.request.fingerprint, settings.policy))
        f.replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.Prepared
        val uncommittedDispatch = f.writes.receiveMessage()
        uncommittedDispatch.events.head.payload shouldBe OperationEvent.DispatchStarted(1, None)
        f.calls.expectNoMessage(50.millis)
        // The fixture has not stored this dispatch. Stop before its acknowledgement.
        testKit.stop(f.ref)
        uncommittedDispatch.fail(new IllegalStateException("dispatch never committed"))
        f.ref = testKit.spawn(PersistentOperation.behaviorWithClock(f.id, f.request.operationKey, f.port,
          settings, () => f.now.get()))
        f.commit(OperationEvent.DispatchStarted(1, None))
        val submission = f.calls.receiveMessage()
        submission.kind shouldBe "submit"
        submission.request shouldBe f.request
        submission.result.success(Unknown("lost reply"))
        f.scheduledUnknown("lost reply")
        f.current().phase shouldBe Phase.Reconciling
      } finally f.close()
    }

    "persist schedules and finite Unknown budgets without interpreting NotFound, Idle or errors as failure" in {
      val f = new Fixture("unknown-budget")
      try {
        f.start().result.success(Unknown("lost acceptance"))
        f.scheduledUnknown("lost acceptance")
        val due = f.current().nextCheckAt
        f.recreate()
        f.current().nextCheckAt shouldBe due
        f.calls.expectNoMessage(50.millis)
        f.advance()
        f.query(1).result.success(NotFound())
        f.scheduledUnknown("external history not found")
        f.current().checks shouldBe 1
        f.advance()
        f.query(2).result.success(Idle())
        f.commit(OperationEvent.UnknownObserved(OperationState.Exhausted, None))
        val blocked = f.current()
        blocked.phase shouldBe Phase.Blocked
        blocked.checks shouldBe 2
        blocked.terminal shouldBe None
        f.advance(10.minutes)
        f.calls.expectNoMessage(50.millis)
        f.ref ! Reconcile("different-operation", f.replies.ref)
        f.replies.receiveMessage() shouldBe a[Rejected]
        f.ref ! Reconcile(f.request.operationKey, f.replies.ref)
        f.commit(OperationEvent.Reconciled(1, 1, f.now.get()))
        f.replies.receiveMessage().asInstanceOf[Current].view.checks shouldBe 2
        f.query(3).result.failure(new TimeoutException("external query failed"))
        f.commit(OperationEvent.UnknownObserved(OperationState.Exhausted, None))
        f.current().phase shouldBe Phase.Blocked
        // New defaults cannot replenish the durable policy or its lifetime count.
        f.recreate(nextSettings = settings.copy(maxChecks = 100, manualCheckBudget = 100, maxReconciliations = 100))
        f.current().checks shouldBe 3
        f.ref ! Reconcile(f.request.operationKey, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Rejected].reason should include("budget exhausted")
        f.calls.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
    }

    "hold the uncancellable live-call slot after timeout, then accept its verified late completion" in {
      val f = new Fixture("timeout", settings.copy(ioTimeout = 1.second))
      try {
        val original = f.start()
        f.advance(1.second)
        f.commitBlocked() should include("original call remains outstanding")
        f.current().phase shouldBe Phase.Blocked
        f.ref ! Reconcile(f.request.operationKey, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Rejected].reason should include("still outstanding")
        f.advance(10.minutes)
        f.calls.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
        val terminal = Succeeded(ByteString("late but authoritative"), evidence(f.request))
        original.result.success(Confirmed(terminal))
        f.commit(OperationEvent.TerminalRecorded(terminal))
        f.current().terminal shouldBe Some(terminal)
        f.current().checks shouldBe 0
      } finally f.close()
    }

    "reject foreign key, digest, ledger, task and connector proof without recording a terminal" in {
      val corruptions: Vector[(String, Evidence => Evidence)] = Vector(
        "key" -> ((e: Evidence) => e.copy(operationKey = "another-operation")),
        "digest" -> ((e: Evidence) => e.copy(requestDigest = "0" * 64)),
        "ledger" -> ((e: Evidence) => e.copy(ledgerEpoch = "new-process-with-lost-ledger")),
        "task" -> ((e: Evidence) => e.copy(taskId = Some("another-task"))),
        "missing-task" -> ((e: Evidence) => e.copy(taskId = None)),
        "proof" -> ((e: Evidence) => e.copy(payload = ByteString("untrusted"))),
        "oversized-proof" -> ((e: Evidence) => e.copy(payload = ByteString(new Array[Byte](settings.limits.maxEvidenceBytes + 1)))))
      corruptions.foreach { case (name, corrupt) =>
        val f = new Fixture(s"wrong-$name")
        try {
          val proof = evidence(f.request)
          f.start().result.success(Accepted(proof))
          f.commit(OperationEvent.RunningObserved(proof, Some(f.now.get() + settings.checkInterval.toMillis)))
          f.current().phase shouldBe Phase.Ongoing
          f.advance()
          val query = f.query(1)
          query.previous shouldBe Some(proof)
          query.result.success(Confirmed(Succeeded(ByteString("untrusted result"), corrupt(proof))))
          f.commitBlocked()
          f.current().phase shouldBe Phase.Blocked
          f.current().terminal shouldBe None
          f.ref ! Reconcile(f.request.operationKey, f.replies.ref)
          f.commit(OperationEvent.Reconciled(1, 1, f.now.get()))
          f.replies.receiveMessage() shouldBe a[Current]
          val verified = f.query(2)
          verified.previous shouldBe Some(proof) // Bad evidence never replaces the trusted task binding.
          val terminal = Succeeded(ByteString("verified"), proof)
          verified.result.success(Confirmed(terminal))
          f.commit(OperationEvent.TerminalRecorded(terminal))
          f.current().terminal shouldBe Some(terminal)
        } finally f.close()
      }
    }

    "fail closed without connector proof and require separately authorized, bounded same-key resubmission" in {
      val untrusted = new Fixture("default-proof", trustedProof = false)
      try {
        untrusted.start().result.success(Confirmed(Succeeded(ByteString("not trusted"), evidence(untrusted.request))))
        untrusted.commitBlocked() should include("not configured")
        untrusted.current().terminal shouldBe None
      } finally untrusted.close()

      val denied = new Fixture("default-resubmission", settings.copy(maxSubmissions = 2))
      try {
        denied.start().result.success(Unknown("lost reply"))
        denied.scheduledUnknown("lost reply")
        denied.current().phase shouldBe Phase.Reconciling
        denied.advance()
        denied.query(1).result.success(SafeToSubmit(evidence(denied.request)))
        denied.commitBlocked() should include("not authorized")
        denied.current().phase shouldBe Phase.Blocked
        denied.calls.expectNoMessage(50.millis)
      } finally denied.close()

      val allowed = new Fixture("authorized-resubmission", settings.copy(maxSubmissions = 2), authorizeResubmission = true)
      try {
        allowed.start().result.success(Unknown("lost reply"))
        allowed.scheduledUnknown("lost reply")
        allowed.current().phase shouldBe Phase.Reconciling
        allowed.advance()
        val proof = evidence(allowed.request)
        allowed.query(1).result.success(SafeToSubmit(proof))
        val pending = allowed.writes.receiveMessage()
        pending.events.head.payload shouldBe OperationEvent.DispatchStarted(2, Some(proof))
        allowed.calls.expectNoMessage(50.millis)
        pending.release()
        val resubmission = allowed.calls.receiveMessage()
        resubmission.kind shouldBe "submit"
        resubmission.request shouldBe allowed.request
        resubmission.result.success(Unknown("still unknown"))
        allowed.scheduledUnknown("still unknown")
        allowed.current().phase shouldBe Phase.Reconciling
        allowed.advance()
        allowed.query(2).result.success(SafeToSubmit(proof))
        allowed.commitBlocked() should include("submission budget exhausted")
        allowed.current().terminal shouldBe None
        allowed.calls.expectNoMessage(50.millis)
      } finally allowed.close()
    }

    "refuse payload/metadata conflicts and remain queryable without I/O under an incompatible connector" in {
      val f = new Fixture("identity", settings.copy(limits = ProtocolLimits(maxRequestBytes = 4)))
      try {
        val invalid = Vector(f.request.copy(payload = ByteString("large")), f.request.copy(effectKey = ""),
          f.request.copy(stepId = "界" * 1366), f.request.copy(ledgerEpoch = ""),
          f.request.copy(connectorVersion = "unregistered"))
        invalid.foreach { request =>
          f.ref ! Ensure(request, f.replies.ref)
          f.replies.receiveMessage() shouldBe a[Rejected]
        }
        f.writes.expectNoMessage(50.millis)
        f.calls.expectNoMessage(50.millis)
        val original = f.start()
        f.ref ! Ensure(f.request.copy(payload = ByteString("new")), f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Rejected].reason should include("different immutable request")
        f.ref ! Ensure(f.request.copy(reservationId = Some("other")), f.replies.ref)
        f.replies.receiveMessage() shouldBe a[Rejected]
        f.recreate(nextPort = new ProbePort(f.calls.ref, connectorVersion = "2"))
        f.current().phase shouldBe Phase.RecoveryBlocked
        f.current().reason.get should include("version")
        f.ref ! Ensure(f.request, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.RecoveryBlocked
        f.ref ! Reconcile(f.request.operationKey, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Current].view.phase shouldBe Phase.RecoveryBlocked
        original.result.success(Confirmed(Succeeded(ByteString("old"), evidence(f.request))))
        f.calls.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
    }

    "fence old callbacks across a same-ref supervised restart and clean up a revoked owner" in {
      val f = new Fixture("same-ref", restartable = true)
      try {
        val original = f.start()
        f.ref ! Get("__restart-operation-test", f.replies.ref)
        f.ref ! CallFinished(-1L, 1L, Success(Confirmed(Succeeded(ByteString("forged old"), evidence(f.request)))))
        f.ref ! CallTimedOut(-1L, 1L)
        val query = f.query(1)
        original.result.success(Confirmed(Succeeded(ByteString("old generation"), evidence(f.request))))
        f.current().phase shouldBe Phase.Reconciling
        f.current().terminal shouldBe None
        f.writes.expectNoMessage(50.millis)
        val terminal = Succeeded(ByteString("new generation"), evidence(f.request))
        query.result.success(Confirmed(terminal))
        f.commit(OperationEvent.TerminalRecorded(terminal))
        f.current().terminal shouldBe Some(terminal)
      } finally f.close()

      val revoked = new Fixture("revoked")
      var replacement: Option[RunLease] = None
      try {
        val original = revoked.start()
        replacement = Some(RunLease.acquire(PersistentOperation.leaseKey(revoked.id), () => ()))
        original.result.success(Confirmed(Succeeded(ByteString("revoked"), evidence(revoked.request))))
        revoked.current().phase shouldBe Phase.RecoveryBlocked
        revoked.current().terminal shouldBe None
        revoked.advance(20.minutes)
        revoked.calls.expectNoMessage(50.millis)
        revoked.writes.expectNoMessage(50.millis)
      } finally {
        replacement.foreach(_.revoke())
        revoked.close()
      }
    }

    "retain a validated business failure monotonically without further queries" in {
      val f = new Fixture("known-failure")
      try {
        val terminal = KnownFailed("DECLINED", "authoritative decision", ByteString("declined"), evidence(f.request))
        f.start().result.success(Confirmed(terminal))
        f.commit(OperationEvent.TerminalRecorded(terminal))
        f.current().phase shouldBe Phase.Failed
        f.ref ! Reconcile(f.request.operationKey, f.replies.ref)
        f.replies.receiveMessage().asInstanceOf[Current].view.terminal shouldBe Some(terminal)
        f.recreate()
        f.current().terminal shouldBe Some(terminal)
        f.advance(20.minutes)
        f.calls.expectNoMessage(50.millis)
        f.writes.expectNoMessage(50.millis)
      } finally f.close()
    }
  }
}
