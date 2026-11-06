package net.imadz.monarch.persistence.resource

import net.imadz.monarch.persistence.ProtocolLimits
import net.imadz.monarch.persistence.resource.ResourceProtocol._

private[persistence] object ResourceValidation {
  val Ceilings: ProtocolLimits = ProtocolLimits()
  def requireValid(problem: Option[String]): Unit = problem.foreach(p => throw new IllegalArgumentException(p))
  private def first(values: Iterable[Option[String]]): Option[String] = values.iterator.flatten.take(1).toVector.headOption
  private def digest(value: String): Option[String] =
    if (value == null || !value.matches("[0-9a-f]{64}")) Some("invalid resource fingerprint/token") else None

  def units(values: Map[String, Int], limits: ProtocolLimits): Option[String] =
    if (values == null || values.isEmpty) Some("resource units must not be empty")
    else if (values.size > limits.maxMetadataEntries) Some("too many resources in bundle")
    else first(values.map { case (key, n) => limits.text("resource", key)
      .orElse(if (n <= 0) Some("resource units must be positive") else None) })

  def request(value: Request, limits: ProtocolLimits = Ceilings): Option[String] =
    if (value == null) Some("missing reservation request")
    else limits.text("reservation id", value.reservationId)
      .orElse(limits.text("owner operation key", value.ownerOperationKey)).orElse(units(value.units, limits))
      .orElse(if (ResourceProtoConverters.requestToProto(value).serializedSize > limits.maxRequestBytes)
        Some("resource request exceeds aggregate byte limit") else None)

  def grant(value: Grant, limits: ProtocolLimits = Ceilings): Option[String] =
    if (value == null) Some("missing grant")
    else request(Request(value.reservationId, value.ownerOperationKey, value.units), limits)
      .orElse(digest(value.token)).orElse(
        if (ResourceProtoConverters.grantToProto(value).serializedSize > limits.maxResultBytes)
          Some("resource grant exceeds result byte limit") else None)

  def proof(value: Proof, limits: ProtocolLimits = Ceilings): Option[String] =
    if (value == null) Some("missing resource proof")
    else limits.text("proof operation key", value.operationKey)
      .orElse(limits.bytes("resource evidence", value.evidence, limits.maxEvidenceBytes))

  def definition(value: ResourceDefinition): Option[String] = {
    val l = value.settings.limits
    l.text("coordinator id", value.coordinatorId).orElse(units(value.capacities, l)).orElse(
      if (ResourceProtoConverters.definitionToProto(value).serializedSize > l.maxRequestBytes)
        Some("resource definition exceeds aggregate request byte limit") else None)
  }

  def demand(value: Request, definition: ResourceDefinition): Option[String] =
    request(value, definition.settings.limits).orElse {
      if (value.units.exists { case (key, n) => !definition.capacities.get(key).exists(_ >= n) })
        Some("undefined resource or demand exceeds frozen capacity") else None
    }.orElse(grant(ResourceIdentity.grant(definition, value), definition.settings.limits))

  def wire(value: Wire, limits: ProtocolLimits = Ceilings): Option[String] = value match {
    case Reserve(r, to) => request(r, limits).orElse(if (to == null) Some("missing reply actor") else None)
    case Query(id, to) => limits.text("reservation id", id).orElse(if (to == null) Some("missing reply actor") else None)
    case Release(id, token, p, to) => limits.text("reservation id", id).orElse(digest(token))
      .orElse(proof(p, limits)).orElse(if (to == null) Some("missing reply actor") else None)
    case Transfer(id, token, target, owner, values, p, to) => limits.text("reservation id", id)
      .orElse(digest(token)).orElse(request(Request(target, owner, values), limits)).orElse(proof(p, limits))
      .orElse(if (to == null) Some("missing reply actor") else None)
    case Granted(g) => grant(g, limits)
    case Queued(id) => limits.text("reservation id", id)
    case Released(id) => limits.text("reservation id", id)
    case Rejected(id, why) => limits.text("reservation id", id).orElse(limits.text("reason", why))
    case Unknown(id, why) => limits.text("reservation id", id).orElse(limits.text("reason", why))
  }

  def event(value: ResourceEvent): Option[String] = {
    import ResourceEvent._
    val error = value match {
      case Defined(d, hash) => definition(d).orElse(digest(hash))
        .orElse(if (hash != d.fingerprint) Some("resource definition digest mismatch") else None)
      case Enqueued(r) => request(r)
      case GrantRecorded(g) => grant(g)
      case ReleaseRecorded(id, token, p) => Ceilings.text("reservation id", id).orElse(digest(token)).orElse(proof(p))
      case TransferRecorded(id, token, target, p) => Ceilings.text("reservation id", id)
        .orElse(digest(token)).orElse(grant(target)).orElse(proof(p))
    }
    error.orElse(if (ResourceProtoConverters.eventToProto(value).serializedSize > Ceilings.maxEnvelopeBytes)
      Some("resource event exceeds envelope limit") else None)
  }

  def state(value: ResourceState): Option[String] = {
    if (value.version != 1) Some("unsupported resource state version")
    else if (value.invalid.flatMap(Ceilings.text("invalid reason", _)).nonEmpty)
      Some("resource invalid-state diagnostic exceeds its string limit")
    else value.definition match {
      case None => if (value.records.nonEmpty || value.queue.nonEmpty) Some("resource state lacks definition") else None
      case Some(d) =>
        val l = d.settings.limits
        definition(d).orElse(value.invalid.flatMap(l.text("invalid reason", _))).orElse {
          if (value.records.size > d.settings.maxReservations || value.queue.size > d.settings.maxQueued)
            Some("resource state exceeds lifetime record/queue bound")
          else if (value.queue.distinct.size != value.queue.size || value.queue.toSet !=
              value.records.iterator.collect { case (id, r) if !r.granted => id }.toSet)
            Some("resource queue does not match ungranted records")
          else first(value.records.map { case (id, record) =>
            demand(record.request, d).orElse {
              if (id != record.request.reservationId || (!record.granted && record.closure.nonEmpty))
                Some("invalid resource record identity or closure") else None
            }.orElse(record.closure.flatMap { c => digest(c.fingerprint).orElse(c.targetReservationId.flatMap { target =>
              l.text("transfer target", target).orElse(value.records.get(target) match {
                case Some(t) if t.granted && target != id && t.request.units.forall {
                  case (key, n) => record.request.units.get(key).exists(_ >= n)
                } => None
                case _ => Some("invalid transfer tombstone destination")
              })
            }) })
          })
        }.orElse {
          if (value.used.exists { case (key, used) => !d.capacities.get(key).exists(_.toLong >= used) })
            Some("resource capacity overallocated") else None
        }.orElse {
          val size = ResourceProtoConverters.stateToProto(value).serializedSize.toLong
          // Every open record reserves enough bytes for a release digest, even
          // when admission is full. Transfers create a new ID and need new credit.
          val futureReleases = value.records.valuesIterator.count(_.closure.isEmpty).toLong * 128L
          if (size + futureReleases > l.maxStateBytes || size > l.maxEnvelopeBytes)
            Some("resource state byte budget exhausted (includes release tombstone credit)") else None
        }
    }
  }
}
