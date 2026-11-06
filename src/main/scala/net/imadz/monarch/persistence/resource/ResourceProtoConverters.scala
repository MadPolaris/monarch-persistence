package net.imadz.monarch.persistence.resource

import akka.actor.typed.{ActorRef, ActorRefResolver}
import akka.util.ByteString
import com.google.protobuf.{ByteString => ProtoBytes}
import net.imadz.monarch.persistence.ProtocolLimits
import net.imadz.monarch.persistence.resource.ResourceProtocol._
import net.imadz.monarch.persistence.resource.proto._

private[persistence] object ResourceProtoConverters {
  private def known(fields: scalapb.UnknownFieldSet): Unit =
    require(fields == scalapb.UnknownFieldSet.empty, "unknown resource protobuf fields/alternative")
  private def schema(version: Int): Unit = require(version == 1, "unsupported resource schema")
  private def required[A](value: Option[A], name: String): A =
    value.getOrElse(throw new IllegalArgumentException(s"missing resource $name"))
  private def unitsToProto(values: Map[String, Int]): Vector[ResourceUnitPO] =
    values.toVector.sortBy(_._1).map { case (key, n) => ResourceUnitPO(resource = key, units = n) }
  private def unitsFromProto(values: Seq[ResourceUnitPO]): Map[String, Int] = {
    require(values.size <= ResourceValidation.Ceilings.maxMetadataEntries, "too many resource entries")
    values.foreach(v => known(v.unknownFields))
    require(values.map(_.resource).distinct.size == values.size, "duplicate resource entries")
    val result = values.map(v => v.resource -> v.units).toMap
    ResourceValidation.requireValid(ResourceValidation.units(result, ResourceValidation.Ceilings))
    result
  }
  def requestToProto(r: Request): ResourceRequestPO = ResourceRequestPO(
    reservationId = r.reservationId, ownerOperationKey = r.ownerOperationKey, units = unitsToProto(r.units))
  private def requestFromProto(r: ResourceRequestPO): Request = {
    known(r.unknownFields)
    Request(r.reservationId, r.ownerOperationKey, unitsFromProto(r.units))
  }
  def grantToProto(g: Grant): ResourceGrantPO = ResourceGrantPO(reservationId = g.reservationId,
    ownerOperationKey = g.ownerOperationKey, token = g.token, units = unitsToProto(g.units))
  private def grantFromProto(g: ResourceGrantPO): Grant = {
    known(g.unknownFields)
    Grant(g.reservationId, g.ownerOperationKey, g.token, unitsFromProto(g.units))
  }
  private def proofToProto(p: Proof): ResourceProofPO = ResourceProofPO(
    operationKey = p.operationKey, evidence = ProtoBytes.copyFrom(p.evidence.toArray))
  private def proofFromProto(p: ResourceProofPO): Proof = {
    known(p.unknownFields)
    Proof(p.operationKey, ByteString(p.evidence.toByteArray))
  }
  private def settingsToProto(s: ResourceSettings): ResourceSettingsPO = {
    val l = s.limits
    ResourceSettingsPO(maxReservations = s.maxReservations, maxQueued = s.maxQueued, limits = Some(ResourceLimitsPO(
      maxRequestBytes = l.maxRequestBytes, maxStateBytes = l.maxStateBytes, maxResultBytes = l.maxResultBytes,
      maxEvidenceBytes = l.maxEvidenceBytes, maxEnvelopeBytes = l.maxEnvelopeBytes, maxStringBytes = l.maxStringBytes,
      maxPlanSteps = l.maxPlanSteps, maxMetadataEntries = l.maxMetadataEntries)))
  }
  private def settingsFromProto(s: ResourceSettingsPO): ResourceSettings = {
    known(s.unknownFields)
    val l = required(s.limits, "limits"); known(l.unknownFields)
    ResourceSettings(s.maxReservations, s.maxQueued, ProtocolLimits(l.maxRequestBytes, l.maxStateBytes,
      l.maxResultBytes, l.maxEvidenceBytes, l.maxEnvelopeBytes, l.maxStringBytes, l.maxPlanSteps, l.maxMetadataEntries))
  }
  def definitionToProto(d: ResourceDefinition): ResourceDefinitionPO = ResourceDefinitionPO(
    coordinatorId = d.coordinatorId, capacities = unitsToProto(d.capacities), settings = Some(settingsToProto(d.settings)))
  private def definitionFromProto(d: ResourceDefinitionPO): ResourceDefinition = {
    known(d.unknownFields)
    ResourceDefinition(d.coordinatorId, unitsFromProto(d.capacities), settingsFromProto(required(d.settings, "settings")))
  }
  private def releaseToProto(id: String, token: String, proof: Proof): ResourceReleaseRecordedPO =
    ResourceReleaseRecordedPO(reservationId = id, token = token, proof = Some(proofToProto(proof)))

  def eventToProto(value: ResourceEvent): ResourceEventPO = {
    import ResourceEvent._
    val event = value match {
      case Defined(d, hash) => ResourceEventPO.Event.DefinitionRecorded(ResourceDefinedPO(
        definition = Some(definitionToProto(d)), fingerprint = hash))
      case Enqueued(r) => ResourceEventPO.Event.Enqueued(requestToProto(r))
      case GrantRecorded(g) => ResourceEventPO.Event.GrantRecorded(grantToProto(g))
      case ReleaseRecorded(id, token, proof) => ResourceEventPO.Event.ReleaseRecorded(releaseToProto(id, token, proof))
      case TransferRecorded(id, token, target, proof) => ResourceEventPO.Event.TransferRecorded(ResourceTransferRecordedPO(
        reservationId = id, token = token, target = Some(grantToProto(target)), proof = Some(proofToProto(proof))))
    }
    ResourceEventPO(schemaVersion = 1, event = event)
  }
  def eventFromProto(value: ResourceEventPO): ResourceEvent = {
    import ResourceEvent._
    schema(value.schemaVersion); known(value.unknownFields)
    val event: ResourceEvent = value.event match {
      case ResourceEventPO.Event.DefinitionRecorded(p) =>
        known(p.unknownFields)
        Defined(definitionFromProto(required(p.definition, "definition")), p.fingerprint)
      case ResourceEventPO.Event.Enqueued(p) => Enqueued(requestFromProto(p))
      case ResourceEventPO.Event.GrantRecorded(p) => GrantRecorded(grantFromProto(p))
      case ResourceEventPO.Event.ReleaseRecorded(p) =>
        known(p.unknownFields)
        ReleaseRecorded(p.reservationId, p.token, proofFromProto(required(p.proof, "release proof")))
      case ResourceEventPO.Event.TransferRecorded(p) =>
        known(p.unknownFields)
        TransferRecorded(p.reservationId, p.token, grantFromProto(required(p.target, "transfer target")),
          proofFromProto(required(p.proof, "transfer proof")))
      case ResourceEventPO.Event.Empty => throw new IllegalArgumentException("missing resource event alternative")
    }
    ResourceValidation.requireValid(ResourceValidation.event(event))
    event
  }

  def stateToProto(value: ResourceState): ResourceStatePO = ResourceStatePO(schemaVersion = 1,
    stateVersion = value.version, definition = value.definition.map(definitionToProto),
    records = value.records.toVector.sortBy(_._1).map { case (_, r) => ResourceRecordPO(
      request = Some(requestToProto(r.request)), granted = r.granted,
      closure = r.closure.map(c => ResourceClosurePO(fingerprint = c.fingerprint,
        targetReservationId = c.targetReservationId))) }, queue = value.queue, invalid = value.invalid)

  def stateFromProto(value: ResourceStatePO): ResourceState = {
    schema(value.schemaVersion); known(value.unknownFields)
    require(value.records.size <= 10000 && value.queue.size <= 1000, "resource snapshot count limit exceeded")
    val records = value.records.map { r =>
      known(r.unknownFields)
      val request = requestFromProto(required(r.request, "record request"))
      val closure = r.closure.map { c =>
        known(c.unknownFields); ResourceClosure(c.fingerprint, c.targetReservationId)
      }
      request.reservationId -> ResourceRecord(request, r.granted, closure)
    }
    require(records.map(_._1).distinct.size == records.size, "duplicate reservation in resource snapshot")
    val state = ResourceState(value.definition.map(definitionFromProto), records.toMap,
      value.queue.toVector, value.invalid, value.stateVersion)
    ResourceValidation.requireValid(ResourceValidation.state(state))
    state
  }

  def wireToProto(value: Wire, resolver: ActorRefResolver): ResourceWirePO = {
    ResourceValidation.requireValid(ResourceValidation.wire(value))
    def ref(to: ActorRef[Reply]): String = {
      val path = resolver.toSerializationFormat(to)
      ResourceValidation.requireValid(ResourceValidation.Ceilings.text("reply actor", path)); path
    }
    val message = value match {
      case Reserve(r, to) => ResourceWirePO.Message.Reserve(ResourceReservePO(request = Some(requestToProto(r)), replyTo = ref(to)))
      case Query(id, to) => ResourceWirePO.Message.Query(ResourceQueryPO(reservationId = id, replyTo = ref(to)))
      case Release(id, token, proof, to) => ResourceWirePO.Message.Release(ResourceReleasePO(
        release = Some(releaseToProto(id, token, proof)), replyTo = ref(to)))
      case Transfer(id, token, target, owner, units, proof, to) => ResourceWirePO.Message.Transfer(ResourceTransferPO(
        reservationId = id, token = token, toReservationId = target, toOwnerOperationKey = owner,
        units = unitsToProto(units), proof = Some(proofToProto(proof)), replyTo = ref(to)))
      case Granted(g) => ResourceWirePO.Message.Granted(grantToProto(g))
      case Queued(id) => ResourceWirePO.Message.Queued(ResourceIdPO(reservationId = id))
      case Released(id) => ResourceWirePO.Message.Released(ResourceIdPO(reservationId = id))
      case Rejected(id, why) => ResourceWirePO.Message.Rejected(ResourceReasonPO(reservationId = id, reason = why))
      case Unknown(id, why) => ResourceWirePO.Message.Unknown(ResourceReasonPO(reservationId = id, reason = why))
    }
    ResourceWirePO(schemaVersion = 1, message = message)
  }
  def wireFromProto(value: ResourceWirePO, resolver: ActorRefResolver): Wire = {
    schema(value.schemaVersion); known(value.unknownFields)
    def ref(path: String): ActorRef[Reply] = {
      ResourceValidation.requireValid(ResourceValidation.Ceilings.text("reply actor", path))
      resolver.resolveActorRef[Reply](path)
    }
    val wire: Wire = value.message match {
      case ResourceWirePO.Message.Reserve(p) =>
        known(p.unknownFields); Reserve(requestFromProto(required(p.request, "reserve request")), ref(p.replyTo))
      case ResourceWirePO.Message.Query(p) =>
        known(p.unknownFields); Query(p.reservationId, ref(p.replyTo))
      case ResourceWirePO.Message.Release(p) =>
        known(p.unknownFields)
        val r = required(p.release, "release"); known(r.unknownFields)
        Release(r.reservationId, r.token, proofFromProto(required(r.proof, "release proof")), ref(p.replyTo))
      case ResourceWirePO.Message.Transfer(p) =>
        known(p.unknownFields)
        Transfer(p.reservationId, p.token, p.toReservationId, p.toOwnerOperationKey, unitsFromProto(p.units),
          proofFromProto(required(p.proof, "transfer proof")), ref(p.replyTo))
      case ResourceWirePO.Message.Granted(p) => Granted(grantFromProto(p))
      case ResourceWirePO.Message.Queued(p) => known(p.unknownFields); Queued(p.reservationId)
      case ResourceWirePO.Message.Released(p) => known(p.unknownFields); Released(p.reservationId)
      case ResourceWirePO.Message.Rejected(p) => known(p.unknownFields); Rejected(p.reservationId, p.reason)
      case ResourceWirePO.Message.Unknown(p) => known(p.unknownFields); Unknown(p.reservationId, p.reason)
      case ResourceWirePO.Message.Empty => throw new IllegalArgumentException("missing resource wire alternative")
    }
    ResourceValidation.requireValid(ResourceValidation.wire(wire)); wire
  }
}
