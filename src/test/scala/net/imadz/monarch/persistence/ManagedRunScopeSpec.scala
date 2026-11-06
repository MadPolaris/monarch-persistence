package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{BehaviorInterceptor, SupervisorStrategy, TypedActorContext}
import akka.persistence.typed.PersistenceId
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.operation.OperationProtocol
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._

class ManagedRunScopeSpec extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
  akka.persistence.journal.plugin = "controlled-journal"
  controlled-journal.class = "net.imadz.monarch.persistence.ControlledJournal"
  controlled-journal.plugin-dispatcher = "akka.actor.default-dispatcher"
  akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
  akka.actor.allow-java-serialization = off
""")) with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
  private val definition = RunDefinition("managed", "1", "utf8-1", Vector("submit", "next"))
  private def id(): PersistenceId = PersistenceId.ofUniqueId("managed-" + UUID.randomUUID())
  private def request(pid: PersistenceId) = OperationProtocol.Request(pid.id + "/submit", pid.id,
    "submit", "bank", "1", "ledger-1", ByteString("credit:100"))
  private def current(req: OperationProtocol.Request, terminal: Boolean = false): OperationProtocol.Current = {
    val result = if (terminal) Some(OperationProtocol.Succeeded(ByteString("receipt"),
      OperationProtocol.Evidence(req.operationKey, req.fingerprint, req.ledgerEpoch, Some("task-1")))) else None
    OperationProtocol.Current(OperationProtocol.View(req.operationKey, Some(req.fingerprint),
      if (terminal) "Succeeded" else "Ongoing", result, None, None, 0))
  }
  private def start(ref: akka.actor.typed.ActorRef[PersistentMonarch.Command[String]], pid: PersistenceId): Unit = {
    val reply = createTestProbe[RunProtocol.Reply]()
    ref ! RunProtocol.StartOrAttach(pid.id, definition.id, definition.version, definition.codecVersion, ByteString.empty, reply.ref)
    reply.receiveMessage() shouldBe a[RunProtocol.Current]
  }
  private def view(ref: akka.actor.typed.ActorRef[PersistentMonarch.Command[String]]): RunProtocol.View = {
    val reply = createTestProbe[RunProtocol.Reply]()
    ref ! RunProtocol.GetRun(reply.ref)
    reply.receiveMessage().asInstanceOf[RunProtocol.Current].view
  }

  "Managed operation scope" should {
    "commit its frozen reference before attachment and wait for terminal then checkpoint before continuing" in {
      val pid = id()
      val req = request(pid)
      val writes = createTestProbe[ControlledJournal.Pending]()
      val calls = createTestProbe[OperationProtocol.Command]()
      val next = createTestProbe[String]()
      val runtime = new ManagedOperations(_ => calls.ref, ManagedOperationSettings(pollInterval = 30.millis))
      ControlledJournal.probes.put(pid.id, writes.ref)
      val ref = spawn(PersistentMonarch.managedBehavior(pid, definition,
        (step: String, s: String, scope: ManagedStageScope) => step match {
          case "submit" => scope.operations.awaitTerminal(req).map(result => s + result.result.utf8String)
          case _ => next.ref ! s; Future.successful(s + ":next")
        }, StateCodec.stringCodec, runtime))
      val reply = createTestProbe[RunProtocol.Reply]()
      ref ! RunProtocol.StartOrAttach(pid.id, definition.id, definition.version, definition.codecVersion, ByteString.empty, reply.ref)
      writes.receiveMessage().release()
      reply.receiveMessage() shouldBe a[RunProtocol.Current]
      val bind = writes.receiveMessage()
      bind.events.head.payload shouldBe PersistentMonarch.RunAwaiting(RunProtocol.AwaitingRef("submit#0", req.operationKey, req.fingerprint))
      calls.expectNoMessage(100.millis)
      bind.release()
      val first = calls.receiveMessage().asInstanceOf[OperationProtocol.Ensure]
      first.request shouldBe req
      first.replyTo ! current(req)
      val second = calls.receiveMessage().asInstanceOf[OperationProtocol.Ensure]
      view(ref).awaiting.map(_.operationKey) shouldBe Some(req.operationKey)
      next.expectNoMessage(100.millis)
      writes.expectNoMessage(100.millis)
      second.replyTo ! current(req, terminal = true)
      val checkpoint = writes.receiveMessage()
      checkpoint.events.head.payload shouldBe a[PersistentMonarch.StageDone]
      next.expectNoMessage(100.millis)
      checkpoint.release()
      next.expectMessage("receipt")
      writes.receiveMessage().release()
      writes.receiveMessage().release()
      createTestProbe[Unit]().awaitAssert(view(ref).status shouldBe RunProtocol.Status.Completed)
      view(ref).awaiting shouldBe None
      createTestProbe[Unit]().awaitAssert(runtime.activeWaiters shouldBe 0)
      testKit.stop(ref)
      ControlledJournal.probes.remove(pid.id)
    }

    "keep query timeouts pending and clear timers and waiters when the run owner stops" in {
      val pid = id()
      val calls = createTestProbe[OperationProtocol.Command]()
      val next = createTestProbe[String]()
      val runtime = new ManagedOperations(_ => calls.ref,
        ManagedOperationSettings(queryTimeout = 120.millis, pollInterval = 30.millis, maxWaiters = 1))
      val ref = spawn(PersistentMonarch.managedBehavior(pid, definition,
        (step: String, s: String, scope: ManagedStageScope) => if (step == "submit")
          scope.awaitTerminal(request(pid)).map(_ => "confirmed")
        else { next.ref ! s; Future.successful(s) }, StateCodec.stringCodec, runtime))
      start(ref, pid)
      calls.receiveMessage() shouldBe a[OperationProtocol.Ensure]
      calls.receiveMessage() shouldBe a[OperationProtocol.Ensure] // first finite ask timed out, same identity queried again
      view(ref).status shouldBe RunProtocol.Status.AwaitingOperation
      next.expectNoMessage(100.millis)
      testKit.stop(ref)
      createTestProbe[Unit]().awaitAssert(runtime.activeWaiters shouldBe 0)
    }

    "reject an interpreter that swallows an unconfirmed operation instead of checkpointing success" in {
      val pid = id()
      val calls = createTestProbe[OperationProtocol.Command]()
      val next = createTestProbe[String]()
      val runtime = new ManagedOperations(_ => calls.ref)
      val ref = spawn(PersistentMonarch.managedBehavior(pid, definition,
        (step: String, s: String, scope: ManagedStageScope) => {
          if (step == "submit") { scope.awaitTerminal(request(pid)); Future.successful("unsafe") }
          else { next.ref ! s; Future.successful(s) }
        }, StateCodec.stringCodec, runtime))
      start(ref, pid)
      createTestProbe[Unit]().awaitAssert(view(ref).status shouldBe RunProtocol.Status.RecoveryBlocked)
      next.expectNoMessage(100.millis)
      ControlledJournal.storage(pid.id).map(_.payload).exists {
        case _: PersistentMonarch.StageDone | _: PersistentMonarch.StageResolved | _: PersistentMonarch.RunFailedEvt => true
        case _ => false
      } shouldBe false
      testKit.stop(ref)
    }

    "rebind the same cursor after a same-ref restart and ignore the old owner's late result" in {
      val pid = id()
      val req = request(pid)
      val calls = createTestProbe[OperationProtocol.Command]()
      val next = createTestProbe[String]()
      val runtime = new ManagedOperations(_ => calls.ref, ManagedOperationSettings(queryTimeout = 2.seconds, pollInterval = 30.millis))
      def underlying = PersistentMonarch.managedBehavior(pid, definition,
        (step: String, s: String, scope: ManagedStageScope) => if (step == "submit")
          scope.awaitTerminal(req).map(result => result.result.utf8String)
        else { next.ref ! s; Future.successful(s) }, StateCodec.stringCodec, runtime)
      val intercepted = Behaviors.intercept(() => new BehaviorInterceptor[PersistentMonarch.Command[String], PersistentMonarch.Command[String]] {
        override def aroundReceive(ctx: TypedActorContext[PersistentMonarch.Command[String]], message: PersistentMonarch.Command[String],
            target: BehaviorInterceptor.ReceiveTarget[PersistentMonarch.Command[String]]) = message match {
          case PersistentMonarch.RunFailed("test", "restart") => throw new IllegalStateException("restart")
          case _ => target(ctx, message)
        }
      })(underlying)
      val ref = spawn(Behaviors.supervise(intercepted).onFailure[IllegalStateException](SupervisorStrategy.restart))
      start(ref, pid)
      val old = calls.receiveMessage().asInstanceOf[OperationProtocol.Ensure]
      ref ! PersistentMonarch.RunFailed[String]("test", "restart")
      val fresh = calls.receiveMessage().asInstanceOf[OperationProtocol.Ensure]
      fresh.request shouldBe old.request
      old.replyTo ! current(req, terminal = true)
      next.expectNoMessage(100.millis)
      fresh.replyTo ! current(req, terminal = true)
      next.expectMessage("receipt")
      createTestProbe[Unit]().awaitAssert(view(ref).status shouldBe RunProtocol.Status.Completed)
      createTestProbe[Unit]().awaitAssert(runtime.activeWaiters shouldBe 0)
      testKit.stop(ref)
    }

    "bound a connector rejection after adding the run diagnostic prefix" in {
      val pid = id()
      val calls = createTestProbe[OperationProtocol.Command]()
      val runtime = new ManagedOperations(_ => calls.ref)
      val ref = spawn(PersistentMonarch.managedBehavior(pid, definition,
        (_: String, s: String, scope: ManagedStageScope) => scope.awaitTerminal(request(pid)).map(_ => s),
        StateCodec.stringCodec, runtime))
      start(ref, pid)
      val ensure = calls.receiveMessage().asInstanceOf[OperationProtocol.Ensure]
      ensure.replyTo ! OperationProtocol.Rejected(request(pid).operationKey, "x" * ProtocolLimits().maxStringBytes)
      createTestProbe[Unit]().awaitAssert(view(ref).status shouldBe RunProtocol.Status.RecoveryBlocked)
      val observed = view(ref)
      observed.reason.get.getBytes("UTF-8").length should be <= ProtocolLimits().maxStringBytes
      akka.serialization.SerializationExtension(system.classicSystem).serialize(RunProtocol.Current(observed)).isSuccess shouldBe true
      testKit.stop(ref)
    }

    "time out a held reference commit without sending an operation or writing a failure terminal" in {
      val pid = id()
      val writes = createTestProbe[ControlledJournal.Pending]()
      val calls = createTestProbe[OperationProtocol.Command]()
      val runtime = new ManagedOperations(_ => calls.ref)
      ControlledJournal.probes.put(pid.id, writes.ref)
      val ref = spawn(PersistentMonarch.managedBehavior(pid, definition,
        (_: String, s: String, scope: ManagedStageScope) => scope.awaitTerminal(request(pid)).map(_ => s),
        StateCodec.stringCodec, runtime, settings = PersistentMonarchSettings(checkpointTimeout = 200.millis)))
      val reply = createTestProbe[RunProtocol.Reply]()
      ref ! RunProtocol.StartOrAttach(pid.id, definition.id, definition.version, definition.codecVersion, ByteString.empty, reply.ref)
      writes.receiveMessage().release()
      reply.receiveMessage() shouldBe a[RunProtocol.Current]
      val held = writes.receiveMessage()
      held.events.head.payload shouldBe a[PersistentMonarch.RunAwaiting]
      calls.expectNoMessage(400.millis)
      held.release()
      createTestProbe[Unit]().awaitAssert(view(ref).status shouldBe RunProtocol.Status.RecoveryBlocked)
      calls.expectNoMessage(100.millis)
      ControlledJournal.storage(pid.id).map(_.payload).exists(_.isInstanceOf[PersistentMonarch.RunFailedEvt]) shouldBe false
      createTestProbe[Unit]().awaitAssert(runtime.activeWaiters shouldBe 0)
      testKit.stop(ref)
      ControlledJournal.probes.remove(pid.id)
    }

    "bound shared waiting capacity and pause overload without writing a business failure" in {
      val firstId = id()
      val secondId = id()
      val calls = createTestProbe[OperationProtocol.Command]()
      val runtime = new ManagedOperations(_ => calls.ref,
        ManagedOperationSettings(queryTimeout = 250.millis, pollInterval = 50.millis, maxWaiters = 1))
      def host(pid: PersistenceId) = PersistentMonarch.managedBehavior(pid, definition,
        (step: String, s: String, scope: ManagedStageScope) => if (step == "submit")
          scope.awaitTerminal(request(pid)).map(_ => s) else Future.successful(s), StateCodec.stringCodec, runtime)
      val first = spawn(host(firstId))
      start(first, firstId)
      calls.receiveMessage() shouldBe a[OperationProtocol.Ensure]
      runtime.activeWaiters shouldBe 1
      val second = spawn(host(secondId))
      start(second, secondId)
      createTestProbe[Unit]().awaitAssert(view(second).status shouldBe RunProtocol.Status.RecoveryBlocked)
      runtime.activeWaiters shouldBe 1
      ControlledJournal.storage(secondId.id).map(_.payload).exists(_.isInstanceOf[PersistentMonarch.RunFailedEvt]) shouldBe false
      testKit.stop(first)
      testKit.stop(second)
      createTestProbe[Unit]().awaitAssert(runtime.activeWaiters shouldBe 0)
    }

    "block recovery if unchanged run versions produce a different operation request" in {
      val pid = id()
      val req = request(pid)
      val calls = createTestProbe[OperationProtocol.Command]()
      val runtime = new ManagedOperations(_ => calls.ref, ManagedOperationSettings(queryTimeout = 200.millis))
      def host(value: OperationProtocol.Request) = PersistentMonarch.managedBehavior(pid, definition,
        (step: String, s: String, scope: ManagedStageScope) => if (step == "submit") scope.awaitTerminal(value).map(_ => s)
          else Future.successful(s), StateCodec.stringCodec, runtime)
      val ref = spawn(host(req))
      start(ref, pid)
      calls.receiveMessage() shouldBe a[OperationProtocol.Ensure]
      testKit.stop(ref)
      val recovered = spawn(host(req.copy(payload = ByteString("changed"))))
      createTestProbe[Unit]().awaitAssert(view(recovered).status shouldBe RunProtocol.Status.RecoveryBlocked)
      calls.expectNoMessage(100.millis)
      view(recovered).awaiting.map(_.requestDigest) shouldBe Some(req.fingerprint)
      testKit.stop(recovered)
    }
  }
}
