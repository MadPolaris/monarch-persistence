package net.imadz.monarch.persistence

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.{CopyOnWriteArrayList, LinkedBlockingQueue, TimeUnit}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Owns only processes and directories created for a single recovery test. */
private[persistence] object RecoveryProcessFixture {
  def classpath(loader: ClassLoader): String = {
    def urls(current: ClassLoader): Vector[String] = if (current == null) Vector.empty else {
      val own = current match {
        case value: java.net.URLClassLoader => value.getURLs.toVector.filter(_.getProtocol == "file").map(u => new File(u.toURI).getAbsolutePath)
        case _ => Vector.empty
      }
      own ++ urls(current.getParent)
    }
    (urls(loader) ++ System.getProperty("java.class.path").split(File.pathSeparator)).distinct.mkString(File.pathSeparator)
  }

  def directory(prefix: String): Path = sys.props.get("monarch.validation.evidence") match {
    case Some(value) =>
      val root = Paths.get(value)
      require(root.isAbsolute, "evidence directory must be absolute")
      Files.createDirectories(root)
      Files.createTempDirectory(root, prefix)
    case None => Files.createTempDirectory(prefix)
  }

  final class Child private[RecoveryProcessFixture] (val process: Process, directory: Path, label: String) {
    val transcript = new CopyOnWriteArrayList[String]()
    private val lines = new LinkedBlockingQueue[String]()
    private val reader = new Thread(() => {
      val input = new BufferedReader(new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8))
      val output = Files.newBufferedWriter(directory.resolve(s"$label-${process.pid()}.log"), StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW)
      def nextLine(): String = try input.readLine() catch {
        case _: java.io.IOException if !process.isAlive => null // Process.destroyForcibly may close the read pipe.
      }
      try {
        var line = nextLine()
        while (line != null) {
          transcript.add(line); lines.put(line)
          output.write(line); output.newLine(); output.flush()
          line = nextLine()
        }
      } finally { input.close(); output.close() }
    }, s"recovery-output-$label")
    reader.setDaemon(true)
    reader.start()

    def expect(fragment: String, timeout: FiniteDuration = 20.seconds): String = {
      val deadline = System.nanoTime() + timeout.toNanos
      var found: String = null
      while (found == null && System.nanoTime() < deadline) {
        val line = lines.poll(math.max(1L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
        if (line != null && line.contains(fragment)) found = line
      }
      require(found != null, s"missing '$fragment' from $label\n${transcript.asScala.mkString("\n")}")
      found
    }

    def finish(): Unit = {
      require(process.waitFor(15, TimeUnit.SECONDS), s"$label did not exit\n${transcript.asScala.mkString("\n")}")
      reader.join(2000)
      require(process.exitValue() == 0, s"$label exited ${process.exitValue()}\n${transcript.asScala.mkString("\n")}")
    }

    def stop(): Unit = {
      val forced = process.isAlive
      if (forced) process.destroyForcibly()
      require(process.waitFor(10, TimeUnit.SECONDS), s"$label did not stop")
      reader.join(2000)
      Files.write(directory.resolve("processes.log"),
        s"pid=${process.pid()} label=$label forced=$forced exit=${process.exitValue()} waited=true\n".getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    }
  }

  def start(main: String, args: Vector[String], directory: Path, label: String, loader: ClassLoader): Child = {
    val java = sys.props.getOrElse("monarch.validation.java", System.getProperty("java.home") + "/bin/java")
    val cp = sys.props.getOrElse("monarch.validation.consumer.classpath", classpath(loader))
    val command = Vector(java, "-Xms32m", "-Xmx192m", "-cp", cp, main) ++ args
    new Child(new ProcessBuilder(command: _*).redirectErrorStream(true).start(), directory, label)
  }
}
