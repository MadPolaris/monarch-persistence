package net.imadz.monarch.persistence

import akka.actor.typed.ActorRef
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.journal.AsyncWriteJournal

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.util.Try

/** A non-blocking journal gate: tests observe the write before allowing its ack.
  * No sleeps, Await, or interpreter-entry counters stand in for persistence.
  */
object ControlledJournal {
  final case class Pending(events: Vector[PersistentRepr], release: () => Unit, fail: Throwable => Unit)
  val probes = TrieMap.empty[String, ActorRef[Pending]]
  private[persistence] val storage = TrieMap.empty[String, Vector[PersistentRepr]]
}

class ControlledJournal extends AsyncWriteJournal {
  import ControlledJournal._
  private implicit val ec: scala.concurrent.ExecutionContext = context.dispatcher

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    val writes = messages.map { message =>
      val ack = Promise[Unit]()
      val payload = message.payload.toVector
      def release(): Unit = {
        storage.synchronized {
          val old = storage.getOrElse(message.persistenceId, Vector.empty)
          storage.put(message.persistenceId, old ++ payload)
        }
        ack.trySuccess(())
        ()
      }
      probes.get(message.persistenceId) match {
        case Some(probe) => probe ! Pending(payload, () => release(), ex => { ack.tryFailure(ex); () })
        case None => release()
      }
      ack.future.map(_ => scala.util.Success(()): Try[Unit])
    }
    Future.sequence(writes)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = Future.successful(())

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future.successful(storage.getOrElse(persistenceId, Vector.empty).lastOption.map(_.sequenceNr).getOrElse(0L))

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long,
      max: Long)(recoveryCallback: PersistentRepr => Unit): Future[Unit] = Future {
    storage.getOrElse(persistenceId, Vector.empty)
      .filter(event => event.sequenceNr >= fromSequenceNr && event.sequenceNr <= toSequenceNr)
      .take(math.min(max, Int.MaxValue.toLong).toInt).foreach(recoveryCallback)
  }
}
