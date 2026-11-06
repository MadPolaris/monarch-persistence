package net.imadz.monarch.persistence

import akka.actor.typed.ActorRef
import akka.persistence.{AtomicWrite, PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.snapshot.SnapshotStore
import akka.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.zip.CRC32
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

/** Test-only serialized journal. A successful file write means force(true) completed.
  * One AtomicWrite is one checksummed frame; a torn final frame is ignored on replay.
  * No production plugin, external service, or Java serialization is used.
  */
object SerializedValidationJournal {
  final case class Stored(sequenceNr: Long, serializerId: Int, manifest: String, bytes: Array[Byte])
  final case class Pending(events: Vector[PersistentRepr], release: () => Unit, fail: Throwable => Unit)
  final case class Replay(from: Long, sequences: Vector[Long], release: () => Unit)
  final class Metrics {
    val pending = new AtomicInteger()
    val maxPending = new AtomicInteger()
    val checkpointCount = new AtomicInteger()
    val totalBytes = new AtomicLong()
    val latencyNanos = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
    val checkpointBytes = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
  }
  val storage = TrieMap.empty[String, Vector[Stored]]
  val gates = TrieMap.empty[String, ActorRef[Pending]]
  val replayProbes = TrieMap.empty[String, ActorRef[Replay]]
  val gatedReplay = TrieMap.empty[String, Boolean]
  val metrics = TrieMap.empty[String, Metrics]
  val forceMarkers = TrieMap.empty[String, String => Unit]
  val config = ConfigFactory.parseString("""
    akka.actor.allow-java-serialization = off
    akka.actor.warn-about-java-serializer-usage = on
    akka.persistence.journal.plugin = "serialized-validation-journal"
    serialized-validation-journal.class = "net.imadz.monarch.persistence.SerializedValidationJournal"
    serialized-validation-journal.plugin-dispatcher = "akka.actor.default-dispatcher"
    serialized-validation-journal.delay = 0ms
    serialized-validation-journal.directory = ""
    serialized-validation-journal.hold-cursor = ""
    serialized-validation-journal.hold-event = ""
    serialized-validation-journal.hold-id = ""
    serialized-validation-journal.force-before-hold = false
    akka.persistence.snapshot-store.plugin = "validation-snapshot-store"
    validation-snapshot-store.class = "net.imadz.monarch.persistence.ValidationSnapshotStore"
    validation-snapshot-store.plugin-dispatcher = "akka.actor.default-dispatcher"
    akka.actor.testkit.typed.single-expect-default = 10s
    akka.loglevel = WARNING
  """)
  def clear(id: String): Unit = {
    storage.remove(id); gates.remove(id); replayProbes.remove(id); gatedReplay.remove(id)
    metrics.remove(id); forceMarkers.remove(id)
    ValidationSnapshotStore.snapshots.remove(id); ValidationSnapshotStore.probes.remove(id)
    ValidationSnapshotStore.failSaves.remove(id)
  }
  def journalPath(directory: Path, id: String): Path = directory.resolve(
    java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(id.getBytes("UTF-8")) + ".journal")

  def encodeFrame(records: Vector[Stored]): Array[Byte] = {
    val bytes = new ByteArrayOutputStream(); val out = new DataOutputStream(bytes)
    out.writeInt(records.size)
    records.foreach { r =>
      out.writeLong(r.sequenceNr); out.writeInt(r.serializerId); out.writeUTF(r.manifest)
      out.writeInt(r.bytes.length); out.write(r.bytes)
    }
    out.close(); bytes.toByteArray
  }
  def readFile(path: Path): Vector[Stored] = {
    if (!Files.exists(path)) Vector.empty
    else {
      val input = ByteBuffer.wrap(Files.readAllBytes(path)); val records = Vector.newBuilder[Stored]
      var complete = true
      while (complete && input.remaining() >= 12) {
        val length = input.getInt(); val checksum = input.getLong()
        require(length >= 0 && length <= 64 * 1024 * 1024, "invalid fixture journal frame length")
        if (input.remaining() < length) complete = false
        else {
          val bytes = new Array[Byte](length); input.get(bytes)
          val crc = new CRC32(); crc.update(bytes)
          require(crc.getValue == checksum, "fixture journal checksum mismatch")
          val in = new DataInputStream(new ByteArrayInputStream(bytes))
          val count = in.readInt(); require(count > 0 && count <= 100000)
          (0 until count).foreach { _ =>
            val sequence = in.readLong(); val serializer = in.readInt(); val manifest = in.readUTF()
            val size = in.readInt(); require(size >= 0 && size <= length)
            val payload = new Array[Byte](size); in.readFully(payload)
            records += Stored(sequence, serializer, manifest, payload)
          }
          require(in.available() == 0, "unexpected fixture journal trailing bytes")
        }
      }
      records.result()
    }
  }
}

class SerializedValidationJournal extends AsyncWriteJournal {
  import SerializedValidationJournal._
  import PersistentMonarch._
  private implicit val ec: scala.concurrent.ExecutionContext = context.dispatcher
  private val serialization = SerializationExtension(context.system)
  private val settings = context.system.settings.config.getConfig("serialized-validation-journal")
  private val delay = settings.getDuration("delay").toNanos.nanos
  private val directory = Option(settings.getString("directory")).filter(_.nonEmpty).map(Paths.get(_))
  private val heldCursor = settings.getString("hold-cursor")
  private val heldEvent = settings.getString("hold-event")
  private val heldId = settings.getString("hold-id")
  private val forceBeforeHold = settings.getBoolean("force-before-hold")
  directory.foreach(Files.createDirectories(_))

  private def records(id: String): Vector[Stored] = directory match {
    case Some(dir) => readFile(journalPath(dir, id))
    case None => storage.getOrElse(id, Vector.empty)
  }
  private def append(id: String, encoded: Vector[Stored]): Unit = directory match {
    case None => storage.synchronized { storage.put(id, storage.getOrElse(id, Vector.empty) ++ encoded); () }
    case Some(dir) =>
      val bytes = encodeFrame(encoded); val crc = new CRC32(); crc.update(bytes)
      val frame = ByteBuffer.allocate(12 + bytes.length).putInt(bytes.length).putLong(crc.getValue).put(bytes)
      frame.flip()
      val channel = FileChannel.open(journalPath(dir, id), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
      try { while (frame.hasRemaining) channel.write(frame); channel.force(true) } finally channel.close()
  }
  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    Future.sequence(messages.map { atomic =>
      val events = atomic.payload.toVector
      val encoded = events.map { event =>
        val serializer = serialization.findSerializerFor(event.payload.asInstanceOf[AnyRef])
        Stored(event.sequenceNr, serializer.identifier, Serializers.manifestFor(serializer, event.payload.asInstanceOf[AnyRef]),
          serialization.serialize(event.payload.asInstanceOf[AnyRef]).get)
      }
      val m = metrics.getOrElseUpdate(atomic.persistenceId, new Metrics)
      val checkpoint = events.exists(e => e.payload.isInstanceOf[StageDone] || e.payload.isInstanceOf[StageResolved])
      val started = System.nanoTime()
      if (checkpoint) {
        val pending = m.pending.incrementAndGet(); m.maxPending.accumulateAndGet(pending, Math.max)
      }
      val promise = Promise[Unit]()
      val persisted = new java.util.concurrent.atomic.AtomicBoolean(false)
      def persist(): Unit = if (persisted.compareAndSet(false, true)) {
        append(atomic.persistenceId, encoded)
        events.foreach(e => forceMarkers.get(atomic.persistenceId).foreach(_(s"FORCED ${e.sequenceNr} ${e.payload.getClass.getSimpleName}")))
      }
      def release(): Unit = {
        try {
          persist()
          if (checkpoint) {
            m.pending.decrementAndGet(); m.checkpointCount.incrementAndGet()
            m.latencyNanos.add(System.nanoTime() - started)
            encoded.foreach(r => m.checkpointBytes.add(r.bytes.length))
          }
          m.totalBytes.addAndGet(encoded.map(_.bytes.length.toLong).sum)
          promise.trySuccess(())
        } catch { case scala.util.control.NonFatal(ex) => promise.tryFailure(ex) }
        ()
      }
      val heldByCursor = heldCursor.nonEmpty && events.exists { e => e.payload match {
        case done: StageDone => done.cursor == heldCursor
        case resolved: StageResolved => resolved.cursor == heldCursor
        case _ => false
      }}
      val heldByEvent = heldEvent.nonEmpty && events.exists(_.payload.getClass.getSimpleName == heldEvent)
      val held = (heldId.isEmpty || heldId == atomic.persistenceId) && (heldByCursor || heldByEvent)
      if (held) {
        if (forceBeforeHold) persist()
        val label = if (heldByCursor) heldCursor else heldEvent
        forceMarkers.get(atomic.persistenceId).foreach(_(s"HELD $label forced=$forceBeforeHold"))
      } else gates.get(atomic.persistenceId) match {
        case Some(probe) => probe ! Pending(events, () => release(), ex => {
          if (checkpoint) m.pending.decrementAndGet()
          promise.tryFailure(ex); ()
        })
        case None if delay.length > 0 => context.system.scheduler.scheduleOnce(delay)(release())
        case None => release()
      }
      promise.future.map(_ => scala.util.Success(()): Try[Unit])
    })
  }
  override def asyncDeleteMessagesTo(id: String, to: Long): Future[Unit] =
    Future.failed(new UnsupportedOperationException("validation never deletes journal events"))
  override def asyncReadHighestSequenceNr(id: String, from: Long): Future[Long] =
    Future.successful(records(id).lastOption.map(_.sequenceNr).getOrElse(0L))
  override def asyncReplayMessages(id: String, from: Long, to: Long, max: Long)(callback: PersistentRepr => Unit): Future[Unit] = {
    val selected = records(id).filter(r => r.sequenceNr >= from && r.sequenceNr <= to).take(math.min(max, Int.MaxValue).toInt)
    val promise = Promise[Unit]()
    def release(): Unit = {
      Try { selected.foreach { r =>
        callback(PersistentRepr(serialization.deserialize(r.bytes, r.serializerId, r.manifest).get, r.sequenceNr, id))
      }}.fold(promise.tryFailure, _ => promise.trySuccess(())); ()
    }
    replayProbes.get(id).foreach(_ ! Replay(from, selected.map(_.sequenceNr), () => release()))
    if (!gatedReplay.getOrElse(id, false)) release()
    promise.future
  }
}

object ValidationSnapshotStore {
  final case class Notice(kind: String, sequence: Long, stored: Option[SerializedValidationJournal.Stored])
  val snapshots = TrieMap.empty[String, Vector[(SnapshotMetadata, SerializedValidationJournal.Stored)]]
  val probes = TrieMap.empty[String, ActorRef[Notice]]
  val failSaves = TrieMap.empty[String, Boolean]
}
class ValidationSnapshotStore extends SnapshotStore {
  import ValidationSnapshotStore._
  import SerializedValidationJournal.Stored
  private val serialization = SerializationExtension(context.system)
  override def loadAsync(id: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val selected = snapshots.getOrElse(id, Vector.empty).filter { case (m, _) =>
      m.sequenceNr <= criteria.maxSequenceNr && m.sequenceNr >= criteria.minSequenceNr &&
      m.timestamp <= criteria.maxTimestamp && m.timestamp >= criteria.minTimestamp
    }.sortBy(_._1.sequenceNr).lastOption
    probes.get(id).foreach(_ ! Notice("load", selected.map(_._1.sequenceNr).getOrElse(0L), selected.map(_._2)))
    Future.fromTry(Try(selected.map { case (metadata, stored) =>
      SelectedSnapshot(metadata, serialization.deserialize(stored.bytes, stored.serializerId, stored.manifest).get)
    }))
  }
  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    if (failSaves.getOrElse(metadata.persistenceId, false)) {
      probes.get(metadata.persistenceId).foreach(_ ! Notice("save-failed", metadata.sequenceNr, None))
      Future.failed(new IllegalStateException("injected snapshot save failure"))
    } else Future.fromTry(Try {
      val serializer = serialization.findSerializerFor(snapshot.asInstanceOf[AnyRef])
      val stored = Stored(metadata.sequenceNr, serializer.identifier, Serializers.manifestFor(serializer, snapshot.asInstanceOf[AnyRef]),
        serialization.serialize(snapshot.asInstanceOf[AnyRef]).get)
      snapshots.synchronized {
        snapshots.put(metadata.persistenceId, snapshots.getOrElse(metadata.persistenceId, Vector.empty) :+ (metadata -> stored))
      }
      probes.get(metadata.persistenceId).foreach(_ ! Notice("saved", metadata.sequenceNr, Some(stored)))
    })
  }
  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful(())
  override def deleteAsync(id: String, criteria: SnapshotSelectionCriteria): Future[Unit] = Future.successful(())
}
