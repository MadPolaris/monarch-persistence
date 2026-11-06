package net.imadz.monarch.persistence.delivery

import akka.actor.typed.ActorRef
import akka.util.ByteString
import net.imadz.monarch.persistence.{ProtocolDigest, RunProtocol, RunSubmission}

import scala.util.Try

/** One stable delivery transfers responsibility, not proof of business completion.
  * The parent keeps its own checkpoint/outbox; this inbox never copies its stages.
  */
object DeliveryProtocol {
  final case class Request(deliveryId: String, sourceRunId: String, target: String,
      child: RunSubmission, payload: ByteString) {
    def fingerprint: String = ProtocolDigest.fields(Vector(ByteString("monarch-delivery-request-v1"),
      ByteString(deliveryId), ByteString(sourceRunId), ByteString(target),
      ByteString(child.fingerprint), payload))
  }

  sealed trait Command
  sealed trait Wire
  sealed trait Reply extends Wire
  final case class Deliver(request: Request, replyTo: ActorRef[Reply]) extends Command with Wire
  final case class Get(deliveryId: String, replyTo: ActorRef[Reply]) extends Command with Wire
  final case class Reconcile(deliveryId: String, replyTo: ActorRef[Reply]) extends Command with Wire

  final case class View(deliveryId: String, requestDigest: Option[String], childRunId: Option[String],
      phase: String, result: Option[ByteString], reason: Option[String], reconciling: Boolean = false)
  final case class Current(view: View) extends Reply
  final case class Rejected(deliveryId: String, reason: String) extends Reply

  object Phase {
    val Empty = "Empty"
    val Accepted = "Accepted"
    val Starting = "Starting"
    val Running = "Running"
    val Completed = "Completed"
    val Failed = "Failed"
    val Blocked = "Blocked"
    val RecoveryBlocked = "RecoveryBlocked"
    val values: Set[String] = Set(Empty, Accepted, Starting, Running, Completed, Failed, Blocked, RecoveryBlocked)
    private[persistence] val terminal: Set[String] = Set(Completed, Failed)
  }

  // Local lifecycle messages deliberately have no Wire binding.
  private[persistence] final case class Drive(generation: Long) extends Command
  private[persistence] final case class PollDue(generation: Long, token: Long, dueAt: Long) extends Command
  private[persistence] final case class CallFinished(generation: Long, callId: Long,
      outcome: Try[RunProtocol.Reply]) extends Command
  private[persistence] final case class CallTimedOut(generation: Long, callId: Long) extends Command
}
