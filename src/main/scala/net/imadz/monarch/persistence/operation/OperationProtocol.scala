package net.imadz.monarch.persistence.operation

import akka.actor.typed.ActorRef
import akka.util.ByteString
import net.imadz.monarch.persistence.ProtocolDigest

import scala.util.Try

/** Concrete bytes DTOs. Only Wire (not Command) is a serialization binding.
  * ByteString is immutable; callers must not circumvent its contract with unsafe
  * array wrapping. Operation keys and ledger epochs are business/durable-ledger
  * identities, never actor generations or process identifiers.
  */
object OperationProtocol {
  final case class Request(
      operationKey: String,
      effectKey: String,
      stepId: String,
      connectorId: String,
      connectorVersion: String,
      ledgerEpoch: String,
      payload: ByteString,
      reservationId: Option[String] = None
  ) {
    /** Length framing and an explicit option tag distinguish every field,
      * including None from Some(""). Validation precedes use of this digest.
      */
    def fingerprint: String = ProtocolDigest.fields(Vector(
      ByteString("monarch-operation-request-v1"),
      ByteString(operationKey), ByteString(effectKey), ByteString(stepId),
      ByteString(connectorId), ByteString(connectorVersion), ByteString(ledgerEpoch),
      payload, ByteString(if (reservationId.isDefined) "some" else "none"),
      ByteString(reservationId.getOrElse(""))))
  }

  final case class Evidence(
      operationKey: String,
      requestDigest: String,
      ledgerEpoch: String,
      taskId: Option[String],
      payload: ByteString = ByteString.empty
  )

  sealed trait Terminal {
    def evidence: Evidence
    def result: ByteString
  }
  final case class Succeeded(result: ByteString, evidence: Evidence) extends Terminal
  final case class KnownFailed(code: String, detail: String, result: ByteString, evidence: Evidence) extends Terminal

  object Phase {
    val Empty = "Empty"
    val Prepared = "Prepared"
    val Dispatching = "Dispatching"
    val Ongoing = "Ongoing"
    val Reconciling = "Reconciling"
    val Blocked = "Blocked"
    val Succeeded = "Succeeded"
    val Failed = "Failed"
    val RecoveryBlocked = "RecoveryBlocked"
    val values: Set[String] = Set(Empty, Prepared, Dispatching, Ongoing,
      Reconciling, Blocked, Succeeded, Failed, RecoveryBlocked)
  }

  final case class View(
      operationKey: String,
      requestDigest: Option[String],
      phase: String,
      terminal: Option[Terminal],
      reason: Option[String],
      nextCheckAt: Option[Long],
      checks: Int
  )

  sealed trait Command
  sealed trait Wire
  final case class Ensure(request: Request, replyTo: ActorRef[Reply]) extends Command with Wire
  final case class Get(operationKey: String, replyTo: ActorRef[Reply]) extends Command with Wire
  final case class Reconcile(operationKey: String, replyTo: ActorRef[Reply]) extends Command with Wire

  sealed trait Reply extends Wire
  final case class Current(view: View) extends Reply
  final case class Rejected(operationKey: String, reason: String) extends Reply

  /** Observations are local connector results, not wire commands or journal
    * events. Confirmed still requires validation and a committed terminal event.
    */
  sealed trait Observation
  sealed trait WithEvidence extends Observation { def evidence: Evidence }
  final case class Accepted(evidence: Evidence) extends WithEvidence
  final case class Running(evidence: Evidence) extends WithEvidence
  final case class Confirmed(terminal: Terminal) extends WithEvidence {
    override def evidence: Evidence = terminal.evidence
  }
  final case class SafeToSubmit(evidence: Evidence) extends WithEvidence
  final case class Unknown(reason: String) extends Observation
  final case class NotFound(reason: String = "external history not found") extends Observation
  final case class Idle(reason: String = "external resource is idle") extends Observation

  // Deliberately not Wire. Both receipt and handling check owner + call identity.
  private[persistence] final case class Drive(generation: Long) extends Command
  private[persistence] final case class CheckDue(generation: Long, token: Long, dueAt: Long) extends Command
  private[persistence] final case class CallFinished(generation: Long, callId: Long,
      outcome: Try[Observation]) extends Command
  private[persistence] final case class CallTimedOut(generation: Long, callId: Long) extends Command
}
