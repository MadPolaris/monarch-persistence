package net.imadz.monarch.persistence

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.{AskPattern, Behaviors}
import akka.util.{ByteString, Timeout}
import com.typesafe.config.ConfigFactory
import net.imadz.monarch.persistence.batch.{BatchProtocol, WorkflowHost}
import net.imadz.monarch.persistence.resource.ResourceProtocol
import net.imadz.monarch.Monarch

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Additive bank/Fab complete sample. Controls no external business completion itself. */
object CompleteWorkflowJvmHost {
  def main(args: Array[String]): Unit = {
    require(Runtime.version().feature() == 11, "JDK11 required")
    val directory = args(0); val externalPort = args(1).toInt; val kind = args(2)
    val jobId = args(3); val mode = args(4); val held = args.lift(5).getOrElse("")
    val forced = args.lift(6).exists(_.toBoolean)
    val heldId = args.lift(7).getOrElse("")
    def q(s: String): String = ConfigFactory.parseMap(Map("p" -> s).asJava).getValue("p").render()
    val config = ConfigFactory.parseString(s"""
      serialized-validation-journal.directory = ${q(directory)}
      serialized-validation-journal.hold-event = ${q(held)}
      serialized-validation-journal.hold-id = ${q(heldId)}
      serialized-validation-journal.force-before-hold = $forced
      akka.persistence.snapshot-store.plugin = "akka.persistence.no-snapshot-store"
      monarch.delivery.max-polls = 300
    """).withFallback(SerializedValidationJournal.config).withFallback(ConfigFactory.load())
    val system = ActorSystem[Nothing](Behaviors.empty, "CompleteWorkflowJvmHost", config)
    implicit val ec: ExecutionContext = system.executionContext
    implicit val scheduler: akka.actor.typed.Scheduler = system.scheduler
    implicit val timeout: Timeout = Timeout(5.seconds)
    import AskPattern._
    val io = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
    def output(line: String): Unit = { println(line); System.out.flush() }
    def origin(label: String, cls: Class[_]): Unit = {
      val path = Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI)
      val hash = if (Files.isRegularFile(path)) MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path))
        .iterator.map(b => f"${b & 0xff}%02x").mkString else "classes-directory"
      output(s"WORKFLOW_ORIGIN $label $path $hash")
    }
    origin("core", classOf[Monarch[_, _]]); origin("persistence", classOf[WorkflowHost])
    val namespace = "r2-" + kind
    val program = new CompleteWorkflowProgram(kind, externalPort)(io)
    val host = new WorkflowHost(system, namespace, program)
    val job = host.job(jobId)
    // Generic event marker registration is fixture-only; workflow control never depends on it.
    SerializedValidationJournal.forceMarkers.put(namespace + "|job|" + jobId, output)
    SerializedValidationJournal.forceMarkers.put(namespace + "|resource|domain", output)
    (0 until 40).foreach { n =>
      SerializedValidationJournal.forceMarkers.put(namespace + "|delivery|" + jobId + s"/batch-$n/delivery", output)
      SerializedValidationJournal.forceMarkers.put(namespace + "|operation|" + jobId + s"/batch-$n/execute", output)
    }
    // This fixture is an immutable finite source. Replay offers and EOF on every
    // host incarnation: death before InputClosed must not invent a closed source.
    def replayInput(): Unit = {
      val count = if (kind == "bank") 3 else 2
      (0 until count).foreach { n =>
        val reply = Await.result(job.ask[BatchProtocol.Reply](to => BatchProtocol.Offer(n, s"$jobId-item-$n", ByteString(s"input-$n"),
          if (kind == "bank") "recharge" else "transport-in", if (kind == "bank") "fund-1" else s"foup-$n", to)), 10.seconds)
        require(reply.isInstanceOf[BatchProtocol.Accepted], "input not accepted: " + reply)
      }
      Await.result(job.ask[BatchProtocol.Reply](BatchProtocol.CloseInput), 10.seconds)
    }
    output(s"WORKFLOW_SOURCE $mode replay")
    replayInput()
    val closure = system.systemActorOf(Behaviors.receiveMessage[ResourceProtocol.Reply] {
      case ResourceProtocol.Granted(grant) =>
        output("WORKFLOW_RESOURCES_CLOSED " + grant.units.toVector.sortBy(_._1).mkString(","))
        output("WORKFLOW_DONE")
        system.terminate()
        Behaviors.stopped
      case other =>
        output("WORKFLOW_CLOSURE_FAILED " + other)
        Behaviors.same
    }, "resource-closure")
    var closureRequested = false
    val reply = system.systemActorOf(Behaviors.receiveMessage[BatchProtocol.Reply] {
      case BatchProtocol.Current(view) =>
        output(s"WORKFLOW_STATE ${view.phase} success=${view.succeeded} failed=${view.failed} held=${view.held} active=${view.active}")
        view.batches.foreach(b => output(s"WORKFLOW_BATCH ${b.batchId} ${b.area} ${b.phase} ${b.reason.getOrElse("")}"))
        if (view.phase == "Drained" && !closureRequested) {
          closureRequested = true
          // A test-only full-capacity claim proves every execution/occupancy claim
          // was closed; the retired fixture domain retains this sentinel afterwards.
          host.resourceDomain ! ResourceProtocol.Reserve(ResourceProtocol.Request(
            jobId + "/closure-check", jobId + "/closure-check", program.capacities), closure)
        }
        Behaviors.same
      case other => output("WORKFLOW_REPLY " + other); Behaviors.same
    }, "query")
    val timer = system.scheduler.scheduleWithFixedDelay(50.millis, 150.millis)(new Runnable {
      override def run(): Unit = job ! BatchProtocol.Get(reply)
    })
    try Await.result(system.whenTerminated, 120.seconds)
    finally { timer.cancel(); io.shutdownNow() }
  }
}
