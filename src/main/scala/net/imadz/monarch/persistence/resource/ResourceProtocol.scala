package net.imadz.monarch.persistence.resource

import akka.actor.typed.ActorRef
import akka.util.ByteString
import net.imadz.monarch.persistence.ProtocolDigest

/** Queued is not permission to dispatch. A grant is valid only in this exclusively
  * controlled resource domain; the host must ensure a single effective writer.
  */
object ResourceProtocol {
  sealed trait Command
  sealed trait Wire
  sealed trait Reply extends Wire

  final case class Request(reservationId: String, ownerOperationKey: String, units: Map[String, Int]) {
    def fingerprint: String = ProtocolDigest.fields(Vector(ByteString("monarch-resource-request-v1"),
      ByteString(reservationId), ByteString(ownerOperationKey)) ++ ResourceIdentity.units(units))
  }
  final case class Grant(reservationId: String, ownerOperationKey: String, token: String, units: Map[String, Int])
  final case class Proof(operationKey: String, evidence: ByteString)
  final case class Reserve(request: Request, replyTo: ActorRef[Reply]) extends Command with Wire
  final case class Query(reservationId: String, replyTo: ActorRef[Reply]) extends Command with Wire
  final case class Release(reservationId: String, token: String, proof: Proof, replyTo: ActorRef[Reply])
      extends Command with Wire
  final case class Transfer(reservationId: String, token: String, toReservationId: String,
      toOwnerOperationKey: String, units: Map[String, Int], proof: Proof, replyTo: ActorRef[Reply])
      extends Command with Wire
  final case class Granted(grant: Grant) extends Reply
  final case class Queued(reservationId: String) extends Reply
  final case class Released(reservationId: String) extends Reply
  final case class Rejected(reservationId: String, reason: String) extends Reply
  final case class Unknown(reservationId: String, reason: String) extends Reply

  // No actor refs, promises or timers are persisted. Queued clients poll Query.
  private[persistence] case object Drive extends Command
}

private[persistence] object ResourceIdentity {
  import ResourceProtocol._
  def units(values: Map[String, Int]): Vector[ByteString] = values.toVector.sortBy(_._1).flatMap {
    case (name, count) => Vector(ByteString(name), ByteString(count.toString))
  }
  def grant(definition: ResourceDefinition, request: Request): Grant = Grant(request.reservationId,
    request.ownerOperationKey, ProtocolDigest.fields(Vector(ByteString("monarch-resource-grant-v1"),
      ByteString(definition.fingerprint), ByteString(request.fingerprint))), request.units)
  def release(id: String, token: String, proof: Proof): String = ProtocolDigest.fields(Vector(
    ByteString("monarch-resource-release-v1"), ByteString(id), ByteString(token),
    ByteString(proof.operationKey), proof.evidence))
  def transfer(id: String, token: String, target: Request, proof: Proof): String = ProtocolDigest.fields(Vector(
    ByteString("monarch-resource-transfer-v1"), ByteString(id), ByteString(token),
    ByteString(target.fingerprint), ByteString(proof.operationKey), proof.evidence))
}
