package net.imadz.monarch.persistence

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import PersistentMonarch._

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Separate JVM entrypoint, using only the public library behavior and test journal. */
object DurableJvmValidationHost {
  def main(args: Array[String]): Unit = {
    require(Runtime.version().feature() == 11, "fixture requires JDK11")
    val directory = args(0); val mode = args(1); val id = args(2)
    val config = ConfigFactory.parseString(s"""
      serialized-validation-journal.directory = ${ConfigFactory.parseMap(Map("p" -> directory).asJava).getValue("p").render()}
      serialized-validation-journal.hold-cursor = "${if (mode.startsWith("first")) "tail#2" else ""}"
      serialized-validation-journal.force-before-hold = ${mode == "first-forced"}
    """).withFallback(SerializedValidationJournal.config).withFallback(ConfigFactory.load())
    val system = ActorSystem[Nothing](Behaviors.empty, "DurableJvmValidation", config)
    implicit val ec: scala.concurrent.ExecutionContext = system.executionContext
    def output(line: String): Unit = { System.out.println("VALIDATION " + line); System.out.flush() }
    output("JAVA " + Runtime.version().feature())
    SerializedValidationJournal.forceMarkers.put(id, output)
    val reply = system.systemActorOf(Behaviors.receiveMessage[RunReply] { message =>
      output("REPLY " + message); Behaviors.same
    }, "reply")
    val ref = system.systemActorOf(PersistentMonarch.behavior[String, String](PersistenceId.ofUniqueId(id),
      Vector("a", "resolved", "tail"), (stage, state) => {
        output(s"STAGE $stage input=$state")
        if (stage == "resolved") Future.failed(new IllegalArgumentException("intentional resolution"))
        else Future.successful(state + stage)
      }, StateCodec.stringCodec,
      failurePolicy = Some((_, _, state) => Future.successful(state + "R")),
      observer = new DurableRunObserver[String] {
        override def onCheckpointCommitted(cursor: String, metadata: Map[String, String], state: String): Unit = output(s"ACK $cursor state=$state")
        override def onResolutionCommitted(cursor: String, error: net.imadz.monarch.StageError, state: String): Unit = output(s"RESOLVED_ACK $cursor state=$state")
        override def onRecovery(runId: String, completed: Int): Unit = output(s"RECOVERY $completed")
        override def onCompleted(runId: String): Unit = { output("DONE " + runId); system.terminate() }
      }), "run")
    if (mode.startsWith("first")) ref ! StartRun(id, "", reply)
    Await.result(system.whenTerminated, 45.seconds)
  }
}

class DurableJvmRecoveryValidationSpec extends AnyWordSpec with Matchers {
  private val javaExecutable = sys.props.getOrElse("monarch.validation.java", System.getProperty("java.home") + "/bin/java")
  private val evidenceRoot = sys.props.get("monarch.validation.evidence").map { path =>
    val root = Paths.get(path)
    require(root.isAbsolute, "validation evidence directory must be absolute")
    Files.createDirectories(root)
  }
  private def temporaryDirectory(prefix: String): Path = evidenceRoot match {
    case Some(root) => Files.createTempDirectory(root, prefix)
    case None => Files.createTempDirectory(prefix)
  }
  private def classpath: String = {
    def urls(loader: ClassLoader): Vector[String] = if (loader == null) Vector.empty else {
      val own = loader match {
        case u: java.net.URLClassLoader => u.getURLs.toVector.filter(_.getProtocol == "file").map(u => new java.io.File(u.toURI).getAbsolutePath)
        case _ => Vector.empty
      }
      own ++ urls(loader.getParent)
    }
    (urls(getClass.getClassLoader) ++ System.getProperty("java.class.path").split(java.io.File.pathSeparator)).distinct.mkString(java.io.File.pathSeparator)
  }
  private final class Child(val process: Process, directory: Path, mode: String) {
    val lines = new LinkedBlockingQueue[String]()
    val transcript = new java.util.concurrent.CopyOnWriteArrayList[String]()
    val reader = new Thread(() => {
      val input = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream, "UTF-8"))
      val output = evidenceRoot.map(_ => Files.newBufferedWriter(directory.resolve(mode + "-" + process.pid() + ".log"),
        java.nio.charset.StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW))
      try {
        var line = input.readLine()
        while (line != null) {
          transcript.add(line); lines.put(line)
          output.foreach { writer => writer.write(line); writer.newLine(); writer.flush() }
          line = input.readLine()
        }
      } finally { input.close(); output.foreach(_.close()) }
    }, "durable-jvm-output")
    reader.setDaemon(true); reader.start()
    def expect(fragment: String): String = {
      val deadline = System.nanoTime() + 30.seconds.toNanos
      var result: String = null
      while (result == null && System.nanoTime() < deadline) {
        val line = lines.poll(math.max(1L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
        if (line != null && line.contains(fragment)) result = line
      }
      assert(result != null, s"missing '$fragment'\n${transcript.asScala.mkString("\n")}")
      result
    }
    def kill(): Unit = {
      val forced = process.isAlive
      if (forced) process.destroyForcibly()
      assert(process.waitFor(10, TimeUnit.SECONDS)); reader.join(2000)
      evidenceRoot.foreach { _ =>
        val record = s"pid=${process.pid()} mode=$mode forced=$forced exit=${process.exitValue()} waited=true\n"
        Files.write(directory.resolve("processes.log"), record.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
      }
    }
  }
  private def start(directory: Path, mode: String, id: String): Child = new Child(new ProcessBuilder(
    javaExecutable, "-Xms32m", "-Xmx192m", "-cp", classpath, "net.imadz.monarch.persistence.DurableJvmValidationHost",
    directory.toString, mode, id).redirectErrorStream(true).start(), directory, mode)
  private def cleanup(directory: Path): Unit = {
    if (evidenceRoot.nonEmpty) {
      println("JVM_VALIDATION evidence=" + directory)
      return
    }
    val paths = Files.walk(directory)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists(_)) finally paths.close()
  }

  "An externally killed JDK11 run" should {
    "recover exactly the forced clean/resolved prefix and rerun an uncommitted checkpoint" in {
      val directory = temporaryDirectory("monarch-jvm-uncommitted-")
      var first: Child = null; var second: Child = null
      try {
        first = start(directory, "first-unforced", "jvm-uncommitted")
        first.expect("JAVA 11"); first.expect("ACK a#0 state=a")
        first.expect("RESOLVED_ACK resolved#1 state=aR")
        first.expect("HELD tail#2 forced=false")
        first.transcript.asScala.exists(_.contains("ACK tail#2")) shouldBe false
        first.kill()
        SerializedValidationJournal.readFile(SerializedValidationJournal.journalPath(directory, "jvm-uncommitted")).map(_.sequenceNr) shouldBe Vector(1L, 2L, 3L)
        second = start(directory, "recover", "jvm-uncommitted")
        second.expect("RECOVERY 2"); second.expect("STAGE tail input=aR")
        second.expect("ACK tail#2 state=aRtail"); second.expect("DONE jvm-uncommitted")
        second.process.waitFor(10, TimeUnit.SECONDS) shouldBe true
        second.process.exitValue() shouldBe 0
        second.transcript.asScala.exists(_.contains("STAGE a ")) shouldBe false
        second.transcript.asScala.exists(_.contains("STAGE resolved ")) shouldBe false
        println("JVM_VALIDATION JDK11 force-ack prefix=2 resolvedState=aR uncommittedTailReruns=1 terminal=confirmed")
      } finally { if (first != null) first.kill(); if (second != null) second.kill(); cleanup(directory) }
    }

    "replay a forced checkpoint whose acknowledgement was lost without executing it twice" in {
      val directory = temporaryDirectory("monarch-jvm-lost-ack-")
      var first: Child = null; var second: Child = null
      try {
        first = start(directory, "first-forced", "jvm-lost-ack")
        first.expect("RESOLVED_ACK resolved#1 state=aR"); first.expect("HELD tail#2 forced=true")
        first.transcript.asScala.exists(_.contains("ACK tail#2")) shouldBe false
        first.kill()
        SerializedValidationJournal.readFile(SerializedValidationJournal.journalPath(directory, "jvm-lost-ack")).map(_.sequenceNr) shouldBe Vector(1L, 2L, 3L, 4L)
        second = start(directory, "recover", "jvm-lost-ack")
        second.expect("RECOVERY 3"); second.expect("DONE jvm-lost-ack")
        second.process.waitFor(10, TimeUnit.SECONDS) shouldBe true
        second.process.exitValue() shouldBe 0
        second.transcript.asScala.exists(_.contains("STAGE ")) shouldBe false
        println("JVM_VALIDATION JDK11 lostAck durablePrefix=3 stageReruns=0 terminal=confirmed")
      } finally { if (first != null) first.kill(); if (second != null) second.kill(); cleanup(directory) }
    }
  }
}
