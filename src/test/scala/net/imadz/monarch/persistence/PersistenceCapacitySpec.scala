package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.lang.management.ManagementFactory
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import PersistentMonarch._

/** Explicit opt-in: -Dmonarch.capacity=true. Delayed journal serialization is
  * real, but storage is test-only memory; these are not production TPS numbers.
  */
class PersistenceCapacitySpec extends ScalaTestWithActorTestKit(
    ConfigFactory.parseString("serialized-validation-journal.delay = 1ms")
      .withFallback(SerializedValidationJournal.config).withFallback(ConfigFactory.load()))
    with AnyWordSpecLike with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = system.executionContext

  "Bounded checkpoint capacity" should {
    "keep one write in flight through a delayed serialized journal and recover its full frontier" in {
      if (!sys.props.get("monarch.capacity").contains("true")) cancel("opt-in capacity run")
      val count = sys.props.get("monarch.capacity.stages").fold(10000)(_.toInt)
      val size = sys.props.get("monarch.capacity.bytes").fold(1024)(_.toInt)
      require(count > 0 && count <= 100000)
      require(size >= 16 && size <= 1024 * 1024)
      require(count.toLong * size <= 256L * 1024 * 1024, "capacity sample exceeds 256MiB payload budget")
      val id = PersistenceId.ofUniqueId(s"capacity-$count-$size")
      val padding = "x" * (size - 16)
      val tail = Promise[String]()
      val checkpoints = testKit.createTestProbe[Int]()
      val entries = testKit.createTestProbe[String]()
      val completed = testKit.createTestProbe[String]()
      val codecNanos = new java.util.concurrent.atomic.AtomicLong()
      val codec = StateCodec[String](state => {
        val started = System.nanoTime()
        val bytes = state.getBytes("UTF-8")
        codecNanos.addAndGet(System.nanoTime() - started)
        bytes
      }, bytes => new String(bytes, "UTF-8"))
      def behavior() = PersistentMonarch.behavior[Int, String](id, (0 until count).toVector :+ count,
        (stage, state) => if (stage == count) { entries.ref ! state; tail.future }
          else Future.successful(f"$stage%016d" + padding), codec,
        observer = new DurableRunObserver[String] {
          override def onCheckpointCommitted(cursor: String, metadata: Map[String, String], state: String): Unit =
            checkpoints.ref ! cursor.takeWhile(_ != '#').toInt
          override def onCompleted(id: String): Unit = completed.ref ! id
        })
      def usedHeap = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
      def gcMillis = ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).filter(_ >= 0).sum
      val heapBefore = usedHeap
      val gcBefore = gcMillis
      val start = System.nanoTime()
      val first = testKit.spawn(behavior())
      val replies = testKit.createTestProbe[RunReply]()
      first ! StartRun(id.id, "initial", replies.ref)
      replies.expectMessage(Accepted(id.id))
      (0 until count).foreach(i => checkpoints.expectMessage(120.seconds, i))
      entries.expectMessage(f"${count - 1}%016d" + padding)
      val elapsed = System.nanoTime() - start
      val metrics = SerializedValidationJournal.metrics(id.id)
      metrics.checkpointCount.get() shouldBe count
      metrics.maxPending.get() shouldBe 1
      metrics.pending.get() shouldBe 0
      val heapAfter = usedHeap
      testKit.stop(first)
      val recoveryStart = System.nanoTime()
      val second = testKit.spawn(behavior())
      entries.expectMessage(120.seconds, f"${count - 1}%016d" + padding)
      val recoveryNanos = System.nanoTime() - recoveryStart
      val latencies = metrics.latencyNanos.iterator().asScala.toVector.sorted
      def percentile(p: Double) = latencies(math.min(latencies.size - 1, (latencies.size * p).toInt)).toDouble / 1e6
      println(f"PERSISTENCE_CAPACITY count=$count bytes=$size maxPending=${metrics.maxPending.get()} " +
        f"elapsedMs=${elapsed / 1e6}%.2f checkpointsPerSec=${count * 1e9 / elapsed}%.2f " +
        f"journalP50Ms=${percentile(.50)}%.3f journalP95Ms=${percentile(.95)}%.3f journalP99Ms=${percentile(.99)}%.3f " +
        f"encodeMs=${codecNanos.get() / 1e6}%.3f eventBytes=${metrics.totalBytes.get()} " +
        f"heapBefore=$heapBefore heapAfter=$heapAfter gcMs=${gcMillis - gcBefore} recoveryMs=${recoveryNanos / 1e6}%.2f")
      tail.success("finished")
      checkpoints.expectMessage(120.seconds, count)
      completed.expectMessage(120.seconds, id.id)
      testKit.stop(second)
      SerializedValidationJournal.clear(id.id)
    }
  }
}
