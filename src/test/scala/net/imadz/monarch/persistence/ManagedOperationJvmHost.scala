package net.imadz.monarch.persistence

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.Monarch
import net.imadz.monarch.persistence.operation.{ExternalOperationPort, OperationProtocol, OperationSettings, PersistentOperation}

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Public-library consumer entrypoint. All real test effects live in a separate
  * ExternalTaskServiceFixture process; observers below only print diagnostics.
  */
object ManagedOperationJvmHost {
  def main(args: Array[String]): Unit = {
    require(Runtime.version().feature() == 11, "R1 recovery fixture requires JDK11")
    val directory = args(0)
    val portNumber = args(1).toInt
    val kind = args(2)
    val runId = args(3)
    val mode = args(4)
    val held = args.lift(5).getOrElse("")
    val forced = args.lift(6).exists(_.toBoolean)
    val operationKey = runId + "/submit"
    val operationPid = runId + "-operation"
    val heldCursor = if (held.startsWith("cursor:")) held.stripPrefix("cursor:") else ""
    val heldEvent = if (held.startsWith("event:")) held.stripPrefix("event:") else ""
    def quoted(value: String): String = ConfigFactory.parseMap(Map("p" -> value).asJava).getValue("p").render()
    val config = ConfigFactory.parseString(s"""
      serialized-validation-journal.directory = ${quoted(directory)}
      serialized-validation-journal.hold-cursor = ${quoted(heldCursor)}
      serialized-validation-journal.hold-event = ${quoted(heldEvent)}
      serialized-validation-journal.hold-id = ${quoted(if (heldEvent.nonEmpty) operationPid else "")}
      serialized-validation-journal.force-before-hold = $forced
      akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
    """).withFallback(SerializedValidationJournal.config).withFallback(ConfigFactory.load())
    val system = ActorSystem[Nothing](Behaviors.empty, "ManagedOperationJvmHost", config)
    implicit val ec: ExecutionContext = system.executionContext
    val executor = Executors.newFixedThreadPool(2)
    val io = ExecutionContext.fromExecutorService(executor)
    def output(value: String): Unit = { println(value); System.out.flush() }
    def origin(label: String, cls: Class[_]): Unit = {
      val path = Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI)
      val sha = if (Files.isRegularFile(path)) MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path))
        .iterator.map(b => f"${b & 0xff}%02x").mkString else "classes-directory"
      output(s"HOST_ORIGIN $label path=$path sha256=$sha")
    }
    output(s"HOST_JAVA ${Runtime.version().feature()} pid=${ProcessHandle.current().pid()}")
    origin("core", classOf[Monarch[_, _]])
    origin("persistence", PersistentMonarch.getClass)
    SerializedValidationJournal.forceMarkers.put(runId, output)
    SerializedValidationJournal.forceMarkers.put(operationPid, output)

    val request = OperationProtocol.Request(operationKey, runId + "/effect", "submit", kind, "fixture-1", "ledger-1",
      ByteString(if (kind == "bank") "CREDIT:account-1:100" else "PROCESS:FOUP-1:recipe-1:wafer-1"))
    val expectedResult = if (kind == "bank") "bank-receipt:" + operationKey else "fab-measurement:" + operationKey
    val connector = new ExternalOperationPort {
      import OperationProtocol._
      override val connectorId: String = kind
      override val connectorVersion: String = "fixture-1"
      private def observe(req: Request, values: Vector[String], query: Boolean): Observation = {
        def evidence(task: String, epoch: String, result: Option[String]): Evidence = {
          val key = if (query) values(values.size - 2) else req.operationKey
          val digest = if (query) values.last else req.fingerprint
          Evidence(key, digest, epoch, Some(task), ByteString(result.getOrElse("running")))
        }
        values.headOption match {
          case Some("RUNNING") if values.size == (if (query) 5 else 3) =>
            val proof = evidence(values(1), values(2), None)
            if (query) Running(proof) else Accepted(proof)
          case Some("SUCCEEDED") if values.size == (if (query) 6 else 4) =>
            Confirmed(Succeeded(ByteString(values(3)), evidence(values(1), values(2), Some(values(3)))))
          case Some("NOT_FOUND") => NotFound("external ledger history unavailable: " + values.drop(1).mkString(","))
          case _ => Unknown("unrecognized or rejected external reply: " + values.mkString(","))
        }
      }
      override def submit(req: Request): Future[Observation] = Future(blocking {
        observe(req, ExternalTaskServiceFixture.call(portNumber, "SUBMIT", Vector(kind, req.operationKey,
          req.fingerprint, req.ledgerEpoch, req.effectKey, req.payload.utf8String)), query = false)
      })(io)
      override def query(req: Request, lastEvidence: Option[Evidence]): Future[Observation] = Future(blocking {
        observe(req, ExternalTaskServiceFixture.call(portNumber, "QUERY", Vector(req.operationKey)), query = true)
      })(io)
      override def validateEvidence(req: Request, observation: WithEvidence): Either[String, Unit] = {
        val proof = observation.evidence
        val validPayload = observation match {
          case Confirmed(Succeeded(result, _)) => result.utf8String == expectedResult && proof.payload == result
          case _: Accepted | _: Running => proof.payload.utf8String == "running"
          case _ => false
        }
        if (req == request && proof.taskId.exists(_.startsWith("task-")) && validPayload) Right(())
        else Left("fixture task/result/recipe binding mismatch")
      }
    }
    val operation = system.systemActorOf(PersistentOperation.behavior(PersistenceId.ofUniqueId(operationPid), operationKey,
      connector, OperationSettings(ioTimeout = 3.seconds, checkInterval = 250.millis, maxChecks = 40)), "operation")
    val operations = new ManagedOperations(_ => operation, ManagedOperationSettings(queryTimeout = 2.seconds, pollInterval = 100.millis))
    val definition = RunDefinition("r1-" + kind, "1", "utf8-1", Vector("submit", "next"))
    val runRef = system.systemActorOf(PersistentMonarch.managedBehavior(PersistenceId.ofUniqueId(runId), definition,
      (step: String, state: String, scope: ManagedStageScope) => if (step == "submit") {
        output("HOST_STAGE submit")
        scope.operations.awaitTerminal(request).map(terminal => terminal.result.utf8String)
      } else {
        output("HOST_NEXT " + state)
        Future.successful(state + ":next")
      }, StateCodec.stringCodec, operations, settings = PersistentMonarchSettings(checkpointTimeout = 30.seconds)), "run")

    val operationReply = system.systemActorOf(Behaviors.receiveMessage[OperationProtocol.Reply] { reply =>
      reply match {
        case OperationProtocol.Current(view) => output("HOST_OPERATION " + view.phase + " " + view.reason.getOrElse(""))
        case other => output("HOST_OPERATION_REPLY " + other)
      }
      Behaviors.same
    }, "operation-reply")
    val runReply = system.systemActorOf(Behaviors.receiveMessage[RunProtocol.Reply] { reply =>
      reply match {
        case RunProtocol.Current(view) if view.status == RunProtocol.Status.Completed =>
          output("HOST_DONE " + view.lastState.map(_.utf8String).getOrElse("unavailable"))
          system.terminate()
        case RunProtocol.Current(view) => output("HOST_RUN " + view.status)
        case other => output("HOST_RUN_REPLY " + other)
      }
      Behaviors.same
    }, "run-reply")
    if (mode == "first") runRef ! RunProtocol.StartOrAttach(runId, definition.id, definition.version, definition.codecVersion, ByteString.empty, runReply)
    val polling = system.scheduler.scheduleWithFixedDelay(50.millis, 150.millis)(new Runnable {
      override def run(): Unit = {
        operation ! OperationProtocol.Get(operationKey, operationReply)
        runRef ! RunProtocol.GetRun(runReply)
      }
    })
    try Await.result(system.whenTerminated, 90.seconds)
    finally { polling.cancel(); io.shutdownNow() }
  }
}
