package net.imadz.monarch.persistence.resource

import akka.util.ByteString
import net.imadz.monarch.persistence.ProtocolDigest
import net.imadz.monarch.persistence.resource.ResourceProtocol._

private[persistence] final case class ResourceDefinition(
    coordinatorId: String, capacities: Map[String, Int], settings: ResourceSettings) {
  def fingerprint: String = {
    val l = settings.limits
    val config = Vector(settings.maxReservations, settings.maxQueued, l.maxRequestBytes, l.maxStateBytes,
      l.maxResultBytes, l.maxEvidenceBytes, l.maxEnvelopeBytes, l.maxStringBytes, l.maxPlanSteps, l.maxMetadataEntries)
    ProtocolDigest.fields(Vector(ByteString("monarch-resource-definition-v1"), ByteString(coordinatorId)) ++
      config.map(n => ByteString(n.toString)) ++ ResourceIdentity.units(capacities))
  }
}

private[persistence] sealed trait ResourceEvent
private[persistence] object ResourceEvent {
  final case class Defined(definition: ResourceDefinition, fingerprint: String) extends ResourceEvent
  final case class Enqueued(request: Request) extends ResourceEvent
  final case class GrantRecorded(grant: Grant) extends ResourceEvent
  final case class ReleaseRecorded(reservationId: String, token: String, proof: Proof) extends ResourceEvent
  final case class TransferRecorded(reservationId: String, token: String, target: Grant, proof: Proof)
      extends ResourceEvent
}

// Proof bytes stay in the journal event. The bounded snapshot retains a framed
// command digest and immutable target reference for exact lost-ACK replays.
private[persistence] final case class ResourceClosure(fingerprint: String, targetReservationId: Option[String])
private[persistence] final case class ResourceRecord(request: Request, granted: Boolean = false,
    closure: Option[ResourceClosure] = None) {
  def active: Boolean = granted && closure.isEmpty
}
private[persistence] final case class ResourceState(
    definition: Option[ResourceDefinition] = None,
    records: Map[String, ResourceRecord] = Map.empty,
    queue: Vector[String] = Vector.empty,
    invalid: Option[String] = None,
    version: Int = 1
) {
  def used: Map[String, Long] = records.valuesIterator.filter(_.active).flatMap(_.request.units)
    .foldLeft(Map.empty[String, Long]) { case (acc, (key, n)) => acc.updated(key, acc.getOrElse(key, 0L) + n.toLong) }
  def nextGrant: Option[String] = queue.find(id => fits(records(id).request.units))
  def fits(units: Map[String, Int]): Boolean = definition.exists { d =>
    val occupied = used
    units.forall { case (key, n) => n > 0 && d.capacities.get(key).exists(_.toLong >= occupied.getOrElse(key, 0L) + n) }
  }
}

private[persistence] object ResourceState {
  import ResourceEvent._

  /** Pure replay. Invalid history retains all previous occupancy and blocks the domain. */
  def evolve(state: ResourceState, event: ResourceEvent): ResourceState = {
    def bad(reason: String): ResourceState = state.copy(invalid = state.invalid.orElse(Some(reason)))
    def configuredProblem: Option[String] = state.definition.flatMap { d =>
      val l = d.settings.limits
      val fields = event match {
        case ReleaseRecorded(_, _, proof) => ResourceValidation.proof(proof, l)
        case TransferRecorded(_, _, _, proof) => ResourceValidation.proof(proof, l)
        case _ => None
      }
      fields.orElse {
        if (ResourceProtoConverters.eventToProto(event).serializedSize > l.maxEnvelopeBytes)
          Some("resource event exceeds frozen envelope limit") else None
      }
    }
    if (state.invalid.nonEmpty) state
    else ResourceValidation.event(event).orElse(configuredProblem) match {
      case Some(reason) => bad(reason)
      case None =>
        val next = event match {
          case Defined(d, digest) if state.definition.isEmpty && state.records.isEmpty && state.queue.isEmpty =>
            if (digest != d.fingerprint) bad("resource definition fingerprint mismatch")
            else state.copy(definition = Some(d))
          case Enqueued(request) if state.definition.nonEmpty =>
            val d = state.definition.get
            if (state.records.contains(request.reservationId) || state.records.size >= d.settings.maxReservations ||
                state.queue.size >= d.settings.maxQueued || ResourceValidation.demand(request, d).nonEmpty)
              bad("invalid resource queue admission")
            else state.copy(records = state.records.updated(request.reservationId, ResourceRecord(request)),
              queue = state.queue :+ request.reservationId)
          case GrantRecorded(grant) if state.definition.nonEmpty =>
            val d = state.definition.get
            val request = Request(grant.reservationId, grant.ownerOperationKey, grant.units)
            val existing = state.records.get(grant.reservationId)
            val eligible = existing match {
              case None => state.nextGrant.isEmpty && state.records.size < d.settings.maxReservations
              case Some(record) => !record.granted && record.closure.isEmpty && record.request == request &&
                state.nextGrant.contains(grant.reservationId)
            }
            if (!eligible || ResourceValidation.demand(request, d).nonEmpty || !state.fits(grant.units) ||
                grant != ResourceIdentity.grant(d, request)) bad("invalid resource grant or FIFO frontier")
            else state.copy(records = state.records.updated(grant.reservationId, ResourceRecord(request, granted = true)),
              queue = if (existing.isDefined) state.queue.filterNot(_ == grant.reservationId) else state.queue)
          case ReleaseRecorded(id, token, proof) if state.definition.nonEmpty =>
            state.records.get(id) match {
              case Some(record) if record.active && proof.operationKey == record.request.ownerOperationKey &&
                  token == ResourceIdentity.grant(state.definition.get, record.request).token =>
                state.copy(records = state.records.updated(id, record.copy(closure = Some(ResourceClosure(
                  ResourceIdentity.release(id, token, proof), None)))))
              case _ => bad("invalid resource release frontier or owner")
            }
          case TransferRecorded(id, token, target, proof) if state.definition.nonEmpty =>
            val d = state.definition.get
            val request = Request(target.reservationId, target.ownerOperationKey, target.units)
            state.records.get(id) match {
              case Some(record) if record.active && !state.records.contains(target.reservationId) &&
                  state.records.size < d.settings.maxReservations && proof.operationKey == record.request.ownerOperationKey &&
                  token == ResourceIdentity.grant(d, record.request).token &&
                  target == ResourceIdentity.grant(d, request) && ResourceValidation.demand(request, d).isEmpty &&
                  target.units.forall { case (key, n) => record.request.units.get(key).exists(_ >= n) } =>
                state.copy(records = state.records
                  .updated(id, record.copy(closure = Some(ResourceClosure(
                    ResourceIdentity.transfer(id, token, request, proof), Some(target.reservationId)))))
                  .updated(target.reservationId, ResourceRecord(request, granted = true)))
              case _ => bad("invalid resource transfer frontier, subset or destination")
            }
          case _ => bad("resource event is not valid at the durable frontier")
        }
        if (next.invalid.nonEmpty) next
        else ResourceValidation.state(next).fold(next)(bad)
    }
  }
}
