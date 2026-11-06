package net.imadz.monarch.persistence.batch

import akka.actor.typed.{ActorRef, ActorRefResolver}
import akka.util.ByteString
import com.google.protobuf.{ByteString => ProtoBytes}
import net.imadz.monarch.persistence.{ProtocolDigest, ProtocolLimits, RunSubmission}
import net.imadz.monarch.persistence.batch.BatchProtocol._
import net.imadz.monarch.persistence.batch.proto._
import net.imadz.monarch.persistence.delivery.DeliveryProtocol
import net.imadz.monarch.persistence.resource.ResourceProtocol
import net.imadz.monarch.persistence.workflow._

/** Pure, bounded schema-1 encoding. No actor lookup, clocks or host I/O occur in persistence conversion.
  * Public manifest/completion bytes also fit the child Run's default 1 MiB state/result ceiling.
  */
object BatchCodec {
  private[persistence] val MaxEnvelopeBytes = 32 * 1024 * 1024
  private[persistence] val MaxActiveBytes = 4 * 1024 * 1024
  private[persistence] val MaxWireBytes = 4 * 1024 * 1024
  private[persistence] val MaxValueBytes = 1024 * 1024
  private[persistence] val MaxRecordBytes = 24 * 1024
  private[persistence] val MaxRecords = 1000
  private[persistence] val MaxItems = 100
  private[persistence] val MaxPageItems = 100
  private val MaxAreas = 32
  private val MaxTransitions = 32
  private val MaxMetadata = 64
  private val MaxBatches = 32
  private val MaxItemBytes = 4096
  private val limits = ProtocolLimits()

  private def known(fields: scalapb.UnknownFieldSet): Unit =
    require(fields == scalapb.UnknownFieldSet.empty, "unknown batch protobuf fields/alternative")
  private def schema(version: Int): Unit = require(version == 1, "unsupported batch schema")
  private def required[A](value: Option[A], name: String): A =
    value.getOrElse(throw new IllegalArgumentException(s"missing batch $name"))
  private def text(value: String, name: String): Unit =
    require(limits.text(name, value).isEmpty, s"invalid batch $name")
  private def optionalText(value: Option[String], name: String): Unit = value.foreach(text(_, name))
  private def digest(value: String): Unit =
    require(value != null && value.matches("[0-9a-f]{64}"), "invalid batch digest")
  private def count(value: Long, maximum: Long, name: String): Unit =
    require(value >= 0 && value <= maximum, s"invalid batch $name")
  private def sequence(value: Long): Unit = count(value, MaxRecords - 1, "item sequence")
  private def deadline(value: Long): Unit = require(value >= 0, "negative batch deadline/delay")
  private def bounded[A](values: Seq[A], maximum: Int, name: String, nonEmpty: Boolean = false): Unit =
    require(values != null && values.size <= maximum && (!nonEmpty || values.nonEmpty), s"invalid batch $name count")
  private def unique[A](values: Seq[A], name: String): Unit =
    require(values.distinct.size == values.size, s"duplicate batch $name")
  private def size(value: scalapb.GeneratedMessage, maximum: Int): Unit =
    require(value.serializedSize <= maximum, "batch value exceeds its byte limit")
  private def vectorBytes(value: Vector[Byte], maximum: Int): ProtoBytes = {
    require(value != null && value.size <= maximum, "batch bytes exceed their limit")
    ProtoBytes.copyFrom(value.toArray)
  }
  private def actorBytes(value: ByteString, maximum: Int): ProtoBytes = {
    require(value != null && value.size <= maximum, "batch bytes exceed their limit")
    ProtoBytes.copyFrom(value.toArray)
  }
  private def vectorBytes(value: ProtoBytes, maximum: Int): Vector[Byte] = {
    require(value.size <= maximum, "batch bytes exceed their limit")
    value.toByteArray.toVector
  }
  private def actorBytes(value: ProtoBytes, maximum: Int): ByteString = {
    require(value.size <= maximum, "batch bytes exceed their limit")
    ByteString(value.toByteArray)
  }
  private def encoded(value: scalapb.GeneratedMessage, maximum: Int): ByteString = {
    size(value, maximum); ByteString(value.toByteArray)
  }
  private def input(value: ByteString, maximum: Int): Array[Byte] = {
    require(value != null && value.size <= maximum, "batch envelope exceeds its byte limit")
    value.toArray
  }

  private def intsToProto(values: Map[String, Int], maximum: Int, positive: Boolean): Vector[BatchIntEntryPO] = {
    require(values != null && values.size <= maximum && (!positive || values.nonEmpty), "invalid batch metadata count")
    values.toVector.sortBy(_._1).map { case (key, value) =>
      text(key, "metadata key")
      require(value >= (if (positive) 1 else 0), "invalid batch metadata value")
      BatchIntEntryPO(key = key, value = value)
    }
  }
  private def intsFromProto(values: Seq[BatchIntEntryPO], maximum: Int, positive: Boolean): Map[String, Int] = {
    bounded(values, maximum, "metadata", nonEmpty = positive)
    unique(values.map(_.key), "metadata key")
    val result = values.map { p => known(p.unknownFields); p.key -> p.value }.toMap
    intsToProto(result, maximum, positive)
    result
  }

  /** Length-framed canonical protobuf; only maps are sorted, never area or item vectors. */
  def definitionDigest(definition: BatchDefinition): String = ProtocolDigest.fields(Vector(
    ByteString("monarch-batch-definition-v1"), encoded(definitionToProto(definition), MaxValueBytes)))

  private[persistence] def definitionToProto(value: BatchDefinition): BatchDefinitionPO = {
    require(value != null, "missing batch definition")
    text(value.id, "definition id"); text(value.version, "definition version")
    bounded(value.areas, MaxAreas, "areas", nonEmpty = true); unique(value.areas.map(_.id), "area id")
    val s = value.settings
    require(s.maxActiveItems <= MaxItems && s.maxActiveBytes <= MaxActiveBytes, "batch active capacity exceeds schema-1 limits")
    val l = s.limits
    val policy = BatchSettingsPO(maxAcceptedItems = s.maxAcceptedItems, maxActiveItems = s.maxActiveItems,
      maxActiveBytes = s.maxActiveBytes, maxItemBytes = s.maxItemBytes, maxResultBytes = s.maxResultBytes,
      maxConcurrentBatches = s.maxConcurrentBatches, maxTransitions = s.maxTransitions,
      maxBatchCalls = s.maxBatchCalls, maxReconciliations = s.maxReconciliations,
      pollIntervalMillis = s.pollIntervalMillis, callTimeoutMillis = s.callTimeoutMillis,
      limits = Some(BatchLimitsPO(maxRequestBytes = l.maxRequestBytes, maxStateBytes = l.maxStateBytes,
        maxResultBytes = l.maxResultBytes, maxEvidenceBytes = l.maxEvidenceBytes,
        maxEnvelopeBytes = l.maxEnvelopeBytes, maxStringBytes = l.maxStringBytes,
        maxPlanSteps = l.maxPlanSteps, maxMetadataEntries = l.maxMetadataEntries)))
    val areas = value.areas.map { area =>
      text(area.id, "area id"); text(area.policyVersion, "policy version")
      val batch = area.batch match {
        case BatchPolicy.CountOrWait(max, wait, min, partial) => BatchAreaPO.Policy.CountOrWait(
          BatchCountOrWaitPO(maxItems = max, minItems = min, maxWaitMillis = wait, allowPartialOnClose = partial))
      }
      BatchAreaPO(id = area.id, policyVersion = area.policyVersion, policy = batch,
        resources = intsToProto(area.resources, MaxMetadata, positive = true),
        parallelism = area.parallelism, maxExecutions = area.maxExecutions)
    }
    BatchDefinitionPO(schemaVersion = 1, id = value.id, version = value.version, areas = areas, settings = Some(policy))
  }

  private def claimToProto(value: ResourceClaim): BatchClaimPO = {
    text(value.reservationId, "reservation id"); text(value.ownerOperationKey, "owner operation key")
    text(value.token, "grant token")
    BatchClaimPO(reservationId = value.reservationId, ownerOperationKey = value.ownerOperationKey,
      token = value.token, units = intsToProto(value.units, MaxMetadata, positive = true))
  }
  private def claimFromProto(value: BatchClaimPO): ResourceClaim = {
    known(value.unknownFields)
    val result = ResourceClaim(value.reservationId, value.ownerOperationKey, value.token,
      intsFromProto(value.units, MaxMetadata, positive = true))
    claimToProto(result); result
  }
  private def grantToProto(value: ResourceProtocol.Grant): BatchClaimPO =
    claimToProto(ResourceClaim(value.reservationId, value.ownerOperationKey, value.token, value.units))
  private def grantFromProto(value: BatchClaimPO): ResourceProtocol.Grant = {
    val c = claimFromProto(value)
    ResourceProtocol.Grant(c.reservationId, c.ownerOperationKey, c.token, c.units)
  }
  private def itemToProto(value: Item): BatchItemPO = {
    sequence(value.sequence); text(value.itemId, "item id"); text(value.area, "area")
    text(value.compatibilityKey, "compatibility key")
    bounded(value.lineage, MaxTransitions, "lineage"); value.lineage.foreach(text(_, "lineage entry"))
    bounded(value.carried, MaxTransitions, "carried claims")
    unique(value.carried.map(_.reservationId), "carried reservation id")
    BatchItemPO(sequence = value.sequence, itemId = value.itemId,
      payload = vectorBytes(value.payload, MaxItemBytes), compatibilityKey = value.compatibilityKey, area = value.area,
      executions = intsToProto(value.executions, MaxAreas, positive = false), lineage = value.lineage,
      carried = value.carried.map(claimToProto))
  }
  private def itemFromProto(value: BatchItemPO): Item = {
    known(value.unknownFields)
    bounded(value.carried, MaxTransitions, "carried claims"); bounded(value.lineage, MaxTransitions, "lineage")
    val result = Item(value.sequence, value.itemId, vectorBytes(value.payload, MaxItemBytes), value.compatibilityKey,
      value.area, intsFromProto(value.executions, MaxAreas, positive = false), value.lineage.toVector,
      value.carried.map(claimFromProto).toVector)
    itemToProto(result); result
  }
  private[persistence] def manifestToProto(value: BatchManifest): BatchManifestPO = {
    require(value != null, "missing batch manifest")
    text(value.jobId, "job id"); text(value.batchId, "batch id"); text(value.area, "manifest area")
    text(value.policyVersion, "policy version"); deadline(value.sealedAt)
    bounded(value.items, MaxItems, "manifest items", nonEmpty = true)
    unique(value.items.map(_.sequence), "manifest sequence"); unique(value.items.map(_.itemId), "manifest item id")
    require(value.items.forall(_.area == value.area), "manifest item area mismatch")
    require(value.items.map(_.compatibilityKey).distinct.size == 1, "incompatible manifest items")
    val result = BatchManifestPO(schemaVersion = 1, jobId = value.jobId, batchId = value.batchId, area = value.area,
      policyVersion = value.policyVersion, items = value.items.map(itemToProto), sealedAt = value.sealedAt)
    size(result, MaxValueBytes); result
  }
  private[persistence] def manifestFromProto(value: BatchManifestPO): BatchManifest = {
    schema(value.schemaVersion); known(value.unknownFields)
    bounded(value.items, MaxItems, "manifest items", nonEmpty = true)
    val result = BatchManifest(value.jobId, value.batchId, value.area, value.policyVersion,
      value.items.map(itemFromProto).toVector, value.sealedAt)
    manifestToProto(result); result
  }
  def manifestToBytes(manifest: BatchManifest): ByteString = encoded(manifestToProto(manifest), MaxValueBytes)
  def manifestFromBytes(bytes: ByteString): BatchManifest = manifestFromProto(BatchManifestPO.parseFrom(input(bytes, MaxValueBytes)))

  private def decisionToProto(value: ItemDecision): BatchDecisionPO = {
    text(value.itemId, "decision item id")
    val disposition = value.disposition match {
      case ItemDisposition.Completed(result) => BatchDecisionPO.Disposition.Completed(
        BatchCompletedPO(result = vectorBytes(result, MaxItemBytes)))
      case ItemDisposition.Failed(reason) =>
        text(reason, "failure reason"); BatchDecisionPO.Disposition.Failed(BatchReasonPO(reason = reason))
      case ItemDisposition.Retry(delay) =>
        deadline(delay); BatchDecisionPO.Disposition.Retry(BatchRetryPO(delayMillis = delay))
      case ItemDisposition.Route(area, key, delay) =>
        text(area, "route area"); text(key, "route compatibility key"); deadline(delay)
        BatchDecisionPO.Disposition.Route(BatchRoutePO(area = area, compatibilityKey = key, delayMillis = delay))
      case ItemDisposition.Hold(reason) =>
        text(reason, "hold reason"); BatchDecisionPO.Disposition.Hold(BatchReasonPO(reason = reason))
    }
    BatchDecisionPO(itemId = value.itemId, disposition = disposition)
  }
  private def decisionFromProto(value: BatchDecisionPO): ItemDecision = {
    known(value.unknownFields)
    val disposition = value.disposition match {
      case BatchDecisionPO.Disposition.Completed(p) => known(p.unknownFields); ItemDisposition.Completed(vectorBytes(p.result, MaxItemBytes))
      case BatchDecisionPO.Disposition.Failed(p) => known(p.unknownFields); ItemDisposition.Failed(p.reason)
      case BatchDecisionPO.Disposition.Retry(p) => known(p.unknownFields); ItemDisposition.Retry(p.delayMillis)
      case BatchDecisionPO.Disposition.Route(p) => known(p.unknownFields); ItemDisposition.Route(p.area, p.compatibilityKey, p.delayMillis)
      case BatchDecisionPO.Disposition.Hold(p) => known(p.unknownFields); ItemDisposition.Hold(p.reason)
      case BatchDecisionPO.Disposition.Empty => throw new IllegalArgumentException("missing/unknown batch item disposition")
    }
    val result = ItemDecision(value.itemId, disposition)
    decisionToProto(result); result
  }
  private[persistence] def completionToProto(value: BatchCompletion): BatchCompletionPO = {
    require(value != null, "missing batch completion")
    bounded(value.decisions, MaxItems, "decisions", nonEmpty = true); unique(value.decisions.map(_.itemId), "decision item id")
    val resource = value.resource match {
      case ResourceDisposition.Release(proof) => BatchCompletionPO.Resource.Release(
        BatchReleasePO(proof = vectorBytes(proof, limits.maxEvidenceBytes)))
      case ResourceDisposition.Carry(id, owner, units, proof) =>
        text(id, "carry reservation id"); text(owner, "carry owner")
        val routes = value.decisions.collect { case ItemDecision(_, r: ItemDisposition.Route) => r }
        require(routes.size == value.decisions.size && routes.map(r => (r.area, r.compatibilityKey, r.delayMillis)).distinct.size == 1,
          "carried occupancy requires all items to route together")
        BatchCompletionPO.Resource.Carry(BatchCarryPO(reservationId = id, ownerOperationKey = owner,
          units = intsToProto(units, MaxMetadata, positive = true), proof = vectorBytes(proof, limits.maxEvidenceBytes)))
    }
    if (value.retainCarried) {
      val routes = value.decisions.collect { case ItemDecision(_, r: ItemDisposition.Route) => r }
      require(routes.size == value.decisions.size && routes.map(r => (r.area, r.compatibilityKey, r.delayMillis)).distinct.size == 1 &&
        value.resource.isInstanceOf[ResourceDisposition.Release], "retained occupancy requires one target and release of execution units only")
    }
    val result = BatchCompletionPO(schemaVersion = 1, decisions = value.decisions.map(decisionToProto), resource = resource,
      retainCarried = value.retainCarried)
    size(result, MaxValueBytes); result
  }
  private[persistence] def completionFromProto(value: BatchCompletionPO): BatchCompletion = {
    schema(value.schemaVersion); known(value.unknownFields)
    bounded(value.decisions, MaxItems, "decisions", nonEmpty = true)
    val resource = value.resource match {
      case BatchCompletionPO.Resource.Release(p) => known(p.unknownFields); ResourceDisposition.Release(vectorBytes(p.proof, limits.maxEvidenceBytes))
      case BatchCompletionPO.Resource.Carry(p) =>
        known(p.unknownFields)
        ResourceDisposition.Carry(p.reservationId, p.ownerOperationKey,
          intsFromProto(p.units, MaxMetadata, positive = true), vectorBytes(p.proof, limits.maxEvidenceBytes))
      case BatchCompletionPO.Resource.Empty => throw new IllegalArgumentException("missing/unknown batch resource disposition")
    }
    val result = BatchCompletion(value.decisions.map(decisionFromProto).toVector, resource, value.retainCarried)
    completionToProto(result); result
  }
  def completionToBytes(completion: BatchCompletion): ByteString = encoded(completionToProto(completion), MaxValueBytes)
  def completionFromBytes(bytes: ByteString): BatchCompletion = completionFromProto(BatchCompletionPO.parseFrom(input(bytes, MaxValueBytes)))

  private val itemPhases = Map("Pending" -> BatchItemPhasePO.ITEM_PENDING, "Running" -> BatchItemPhasePO.ITEM_RUNNING,
    "Succeeded" -> BatchItemPhasePO.ITEM_SUCCEEDED, "Failed" -> BatchItemPhasePO.ITEM_FAILED, "Held" -> BatchItemPhasePO.ITEM_HELD)
  private val activePhases = Map("Reserving" -> BatchActivePhasePO.ACTIVE_RESERVING,
    "Preparing" -> BatchActivePhasePO.ACTIVE_PREPARING, "Delivering" -> BatchActivePhasePO.ACTIVE_DELIVERING,
    "WaitingChild" -> BatchActivePhasePO.ACTIVE_WAITING_CHILD,
    "ReconcilingChild" -> BatchActivePhasePO.ACTIVE_RECONCILING_CHILD,
    "ReleasingCarried" -> BatchActivePhasePO.ACTIVE_RELEASING_CARRIED,
    "Releasing" -> BatchActivePhasePO.ACTIVE_RELEASING, "Settling" -> BatchActivePhasePO.ACTIVE_SETTLING,
    "Blocked" -> BatchActivePhasePO.ACTIVE_BLOCKED)
  private val jobPhases = Map("Open" -> BatchJobPhasePO.JOB_OPEN, "Closing" -> BatchJobPhasePO.JOB_CLOSING,
    "ClosingBlocked" -> BatchJobPhasePO.JOB_CLOSING_BLOCKED, "Drained" -> BatchJobPhasePO.JOB_DRAINED,
    "Blocked" -> BatchJobPhasePO.JOB_BLOCKED, "RecoveryBlocked" -> BatchJobPhasePO.JOB_RECOVERY_BLOCKED)
  private def phaseTo[A](value: String, phases: Map[String, A]): A =
    phases.getOrElse(value, throw new IllegalArgumentException("unknown batch phase"))
  private def phaseFrom[A](value: A, phases: Map[String, A]): String =
    phases.collectFirst { case (name, phase) if phase == value => name }
      .getOrElse(throw new IllegalArgumentException("unknown/unspecified batch phase"))

  private[persistence] def recordToProto(value: ItemRecord): BatchRecordPO = {
    digest(value.inputDigest); deadline(value.acceptedAt); deadline(value.dueAt)
    optionalText(value.batchId, "record batch id"); optionalText(value.reason, "record reason")
    require(value.phase != "Running" || value.batchId.nonEmpty, "running item requires a batch id")
    val result = BatchRecordPO(item = Some(itemToProto(value.item)), inputDigest = value.inputDigest,
      phase = phaseTo(value.phase, itemPhases), acceptedAt = value.acceptedAt, dueAt = value.dueAt,
      batchId = value.batchId, result = vectorBytes(value.result, MaxItemBytes), reason = value.reason)
    size(result, MaxRecordBytes); result
  }
  private def recordFromProto(value: BatchRecordPO): ItemRecord = {
    known(value.unknownFields); size(value, MaxRecordBytes)
    val result = ItemRecord(itemFromProto(required(value.item, "record item")), value.inputDigest,
      phaseFrom(value.phase, itemPhases), value.acceptedAt, value.dueAt, value.batchId,
      vectorBytes(value.result, MaxItemBytes), value.reason)
    recordToProto(result); result
  }
  private def submissionToProto(value: RunSubmission): BatchSubmissionPO = {
    require(value.validate(limits).isEmpty, "invalid batch child submission")
    BatchSubmissionPO(runId = value.runId, definitionId = value.definitionId, definitionVersion = value.definitionVersion,
      codecVersion = value.codecVersion, initialState = actorBytes(value.initialState, limits.maxStateBytes))
  }
  private def submissionFromProto(value: BatchSubmissionPO): RunSubmission = {
    known(value.unknownFields)
    val result = RunSubmission(value.runId, value.definitionId, value.definitionVersion, value.codecVersion,
      actorBytes(value.initialState, limits.maxStateBytes))
    submissionToProto(result); result
  }
  private def deliveryToProto(value: DeliveryProtocol.Request): BatchDeliveryPO = {
    text(value.deliveryId, "delivery id"); text(value.sourceRunId, "source run id"); text(value.target, "delivery target")
    BatchDeliveryPO(deliveryId = value.deliveryId, sourceRunId = value.sourceRunId, target = value.target,
      child = Some(submissionToProto(value.child)), payload = actorBytes(value.payload, limits.maxRequestBytes))
  }
  private def deliveryFromProto(value: BatchDeliveryPO): DeliveryProtocol.Request = {
    known(value.unknownFields)
    val result = DeliveryProtocol.Request(value.deliveryId, value.sourceRunId, value.target,
      submissionFromProto(required(value.child, "delivery child")), actorBytes(value.payload, limits.maxRequestBytes))
    deliveryToProto(result); result
  }
  private[persistence] def activeToProto(value: ActiveBatch): BatchActivePO = {
    count(value.releasedCarried, MaxItems * MaxTransitions, "released carried count")
    count(value.calls, 100000, "calls"); count(value.reconciliations, 100, "reconciliations"); deadline(value.nextDue)
    optionalText(value.reason, "active reason")
    val effectivePhase = if (value.phase == "Blocked") {
      require(value.reason.nonEmpty, "blocked batch requires a reason")
      val resume = required(value.resumePhase, "resume phase")
      require(resume != "Blocked", "blocked batch cannot resume into itself")
      resume
    } else {
      require(value.resumePhase.isEmpty, "only a blocked batch has a resume phase")
      value.phase
    }
    phaseTo(effectivePhase, activePhases)
    if (effectivePhase != "Reserving") require(value.grant.nonEmpty, "batch phase requires a grant")
    if (Set("Delivering", "WaitingChild", "ReconcilingChild", "ReleasingCarried", "Releasing", "Settling")(effectivePhase))
      require(value.delivery.nonEmpty, "batch phase requires a delivery")
    if (Set("ReleasingCarried", "Releasing", "Settling")(effectivePhase))
      require(value.completion.nonEmpty, "batch phase requires a completion")
    val carried = value.manifest.items.flatMap(_.carried).distinct
    require(value.releasedCarried <= carried.size, "released carried count exceeds manifest claims")
    value.grant.foreach(g => require(g.reservationId == value.manifest.reservationId &&
      g.ownerOperationKey == value.manifest.operationKey, "grant identity does not match manifest"))
    value.delivery.foreach(d => require(d.deliveryId == value.manifest.deliveryId &&
      d.sourceRunId == value.manifest.jobId && d.child.runId == value.manifest.runId,
      "delivery identity does not match manifest"))
    value.carry.foreach { c =>
      require(value.completion.exists(_.resource match {
        case ResourceDisposition.Carry(id, owner, units, _) =>
          c.reservationId == id && c.ownerOperationKey == owner && c.units == units
        case _ => false
      }), "carried grant does not match completion")
    }
    value.completion.foreach { c =>
      require(c.decisions.map(_.itemId).toSet == value.manifest.items.map(_.itemId).toSet,
        "completion must cover the manifest exactly")
      if (effectivePhase == "Settling") require(c.resource match {
        case _: ResourceDisposition.Carry => value.carry.nonEmpty
        case _: ResourceDisposition.Release => value.carry.isEmpty
      }, "settlement requires the committed release/transfer receipt")
    }
    val result = BatchActivePO(manifest = Some(manifestToProto(value.manifest)), phase = phaseTo(value.phase, activePhases),
      grant = value.grant.map(grantToProto), delivery = value.delivery.map(deliveryToProto),
      completion = value.completion.map(completionToProto), carry = value.carry.map(claimToProto),
      releasedCarried = value.releasedCarried, calls = value.calls, nextDue = value.nextDue, reason = value.reason,
      resumePhase = value.resumePhase.map(phaseTo(_, activePhases)), reconciliations = value.reconciliations)
    size(result, MaxActiveBytes); result
  }
  private def activeFromProto(value: BatchActivePO): ActiveBatch = {
    known(value.unknownFields); size(value, MaxActiveBytes)
    val result = ActiveBatch(manifestFromProto(required(value.manifest, "active manifest")), phaseFrom(value.phase, activePhases),
      value.grant.map(grantFromProto), value.delivery.map(deliveryFromProto), value.completion.map(completionFromProto),
      value.carry.map(claimFromProto), value.releasedCarried, value.calls, value.nextDue, value.reason,
      value.resumePhase.map(phaseFrom(_, activePhases)), value.reconciliations)
    activeToProto(result); result
  }

  private[persistence] def eventToProto(value: BatchEvent): BatchEventPO = {
    import BatchEvent._
    val event = value match {
      case Initialized(id, d) =>
        text(id, "job id"); digest(d)
        BatchEventPO.Event.Initialized(BatchInitializedPO(jobId = id, definitionDigest = d))
      case InputAccepted(record) => BatchEventPO.Event.InputAccepted(recordToProto(record))
      case Sealed(batch) => BatchEventPO.Event.Sealed(activeToProto(batch))
      case Advanced(batch) => BatchEventPO.Event.Advanced(activeToProto(batch))
      case Settled(id, records) =>
        text(id, "settled batch id"); bounded(records, MaxItems, "settled records", nonEmpty = true)
        unique(records.map(_.item.sequence), "settled sequence"); unique(records.map(_.item.itemId), "settled item id")
        BatchEventPO.Event.Settled(BatchSettledPO(batchId = id, records = records.map(recordToProto)))
      case Verified(record) => BatchEventPO.Event.Verified(recordToProto(record))
      case InputClosed => BatchEventPO.Event.InputClosed(BatchInputClosedPO())
    }
    val result = BatchEventPO(schemaVersion = 1, event = event)
    size(result, MaxEnvelopeBytes); result
  }
  private[persistence] def eventFromProto(value: BatchEventPO): BatchEvent = {
    import BatchEvent._
    schema(value.schemaVersion); known(value.unknownFields)
    val result = value.event match {
      case BatchEventPO.Event.Initialized(p) => known(p.unknownFields); Initialized(p.jobId, p.definitionDigest)
      case BatchEventPO.Event.InputAccepted(p) => InputAccepted(recordFromProto(p))
      case BatchEventPO.Event.Sealed(p) => Sealed(activeFromProto(p))
      case BatchEventPO.Event.Advanced(p) => Advanced(activeFromProto(p))
      case BatchEventPO.Event.Settled(p) =>
        known(p.unknownFields); bounded(p.records, MaxItems, "settled records", nonEmpty = true)
        Settled(p.batchId, p.records.map(recordFromProto).toVector)
      case BatchEventPO.Event.Verified(p) => Verified(recordFromProto(p))
      case BatchEventPO.Event.InputClosed(p) => known(p.unknownFields); InputClosed
      case BatchEventPO.Event.Empty => throw new IllegalArgumentException("missing/unknown batch event")
    }
    eventToProto(result); result
  }
  private[persistence] def stateToProto(value: BatchState): BatchStatePO = {
    optionalText(value.jobId, "job id"); value.definitionDigest.foreach(digest); optionalText(value.invalid, "invalid reason")
    count(value.nextSequence, MaxRecords, "next sequence"); deadline(value.nextBatch)
    require(value.records.size <= MaxRecords && value.batches.size <= MaxBatches, "batch state count exceeds its limit")
    require(value.jobId.isDefined == value.definitionDigest.isDefined, "incomplete batch initialization")
    require(value.jobId.nonEmpty || (value.records.isEmpty && value.batches.isEmpty && value.nextSequence == 0 &&
      value.nextBatch == 0 && !value.closed), "uninitialized batch frontier")
    require(value.records.size.toLong == value.nextSequence &&
      value.records.forall { case (key, r) => key == r.item.sequence && key >= 0 && key < value.nextSequence },
      "invalid batch sequence frontier")
    unique(value.records.valuesIterator.map(_.item.itemId).toVector, "state item id")
    val live = value.records.valuesIterator.filterNot(r => r.phase == "Succeeded" || r.phase == "Failed").toVector
    bounded(live, MaxItems, "active items")
    val manifestItems = value.batches.valuesIterator.flatMap(_.manifest.items).toVector
    bounded(manifestItems, MaxItems, "active manifest items")
    unique(manifestItems.map(_.sequence), "active manifest sequence")
    require(value.batches.forall { case (id, batch) =>
      id == batch.manifest.batchId && value.jobId.contains(batch.manifest.jobId) &&
        batch.manifest.items.forall(item => value.records.get(item.sequence).exists(r =>
          r.item == item && r.phase == "Running" && r.batchId.contains(id)))
    }, "active batch does not match item frontier")
    require(value.records.valuesIterator.filter(_.phase == "Running").forall(r =>
      r.batchId.flatMap(value.batches.get).exists(_.manifest.items.contains(r.item))), "orphaned running item")
    require(value.nextBatch >= value.batches.size, "invalid next batch counter")
    val records = value.records.toVector.sortBy(_._1).map { case (key, r) =>
      BatchRecordEntryPO(sequence = key, record = Some(recordToProto(r)))
    }
    val batches = value.batches.toVector.sortBy(_._1).map { case (key, b) =>
      BatchActiveEntryPO(batchId = key, batch = Some(activeToProto(b)))
    }
    val activeRecordBytes = records.iterator.filter(p => value.records(p.sequence).phase != "Succeeded" &&
      value.records(p.sequence).phase != "Failed").map(_.serializedSize.toLong).sum
    require(activeRecordBytes + batches.iterator.map(_.serializedSize.toLong).sum <= MaxActiveBytes,
      "batch active frontier exceeds its byte limit")
    val result = BatchStatePO(schemaVersion = 1, jobId = value.jobId, definitionDigest = value.definitionDigest,
      nextSequence = value.nextSequence, nextBatch = value.nextBatch, closed = value.closed,
      records = records, batches = batches, invalid = value.invalid)
    size(result, MaxEnvelopeBytes); result
  }
  private[persistence] def stateFromProto(value: BatchStatePO): BatchState = {
    schema(value.schemaVersion); known(value.unknownFields)
    bounded(value.records, MaxRecords, "state records"); bounded(value.batches, MaxBatches, "state batches")
    unique(value.records.map(_.sequence), "record key"); unique(value.batches.map(_.batchId), "active batch key")
    val records = value.records.map { p => known(p.unknownFields); p.sequence -> recordFromProto(required(p.record, "record entry")) }.toMap
    val batches = value.batches.map { p => known(p.unknownFields); p.batchId -> activeFromProto(required(p.batch, "active entry")) }.toMap
    val result = BatchState(value.jobId, value.definitionDigest, value.nextSequence, value.nextBatch, value.closed,
      records, batches, value.invalid)
    stateToProto(result); result
  }

  private def itemViewToProto(value: ItemView): BatchItemViewPO = {
    sequence(value.sequence); text(value.itemId, "view item id"); text(value.area, "view area")
    optionalText(value.reason, "view reason")
    BatchItemViewPO(sequence = value.sequence, itemId = value.itemId, area = value.area,
      phase = phaseTo(value.phase, itemPhases), executions = intsToProto(value.executions, MaxAreas, positive = false),
      reason = value.reason, result = actorBytes(value.result, MaxItemBytes))
  }
  private def itemViewFromProto(value: BatchItemViewPO): ItemView = {
    known(value.unknownFields)
    val result = ItemView(value.sequence, value.itemId, value.area, phaseFrom(value.phase, itemPhases),
      intsFromProto(value.executions, MaxAreas, positive = false), value.reason, actorBytes(value.result, MaxItemBytes))
    itemViewToProto(result); result
  }
  private def batchViewToProto(value: BatchView): BatchViewEntryPO = {
    text(value.batchId, "view batch id"); text(value.area, "view batch area")
    bounded(value.itemIds, MaxItems, "view batch items", nonEmpty = true)
    unique(value.itemIds, "view batch item id"); value.itemIds.foreach(text(_, "view item id"))
    optionalText(value.reservationId, "view reservation id"); optionalText(value.deliveryId, "view delivery id")
    optionalText(value.reason, "view batch reason")
    BatchViewEntryPO(batchId = value.batchId, area = value.area, phase = phaseTo(value.phase, activePhases),
      itemIds = value.itemIds, reservationId = value.reservationId, deliveryId = value.deliveryId, reason = value.reason)
  }
  private def batchViewFromProto(value: BatchViewEntryPO): BatchView = {
    known(value.unknownFields)
    val result = BatchView(value.batchId, value.area, phaseFrom(value.phase, activePhases), value.itemIds.toVector,
      value.reservationId, value.deliveryId, value.reason)
    batchViewToProto(result); result
  }
  private[persistence] def viewToProto(value: View): BatchViewPO = {
    text(value.jobId, "view job id"); optionalText(value.reason, "view reason")
    count(value.accepted, MaxRecords, "accepted count"); count(value.succeeded, value.accepted, "succeeded count")
    count(value.failed, value.accepted, "failed count"); count(value.held, value.accepted, "held count")
    count(value.active, math.min(value.accepted, MaxItems.toLong), "active count"); count(value.activeBytes, MaxActiveBytes, "active bytes")
    require(value.succeeded.toLong + value.failed + value.held <= value.accepted &&
      value.succeeded.toLong + value.failed + value.active <= value.accepted, "inconsistent batch view counts")
    bounded(value.items, MaxPageItems, "query rows"); bounded(value.batches, MaxBatches, "view batches")
    require(value.items.size <= value.accepted, "query rows exceed accepted count")
    unique(value.items.map(_.sequence), "view sequence"); unique(value.items.map(_.itemId), "view item id")
    unique(value.batches.map(_.batchId), "view batch id")
    val batchedIds = value.batches.flatMap(_.itemIds)
    bounded(batchedIds, MaxItems, "view active items"); unique(batchedIds, "view active item id")
    BatchViewPO(jobId = value.jobId, phase = phaseTo(value.phase, jobPhases), inputClosed = value.inputClosed,
      accepted = value.accepted, succeeded = value.succeeded, failed = value.failed, held = value.held,
      active = value.active, activeBytes = value.activeBytes, batches = value.batches.map(batchViewToProto),
      items = value.items.map(itemViewToProto), truncated = value.truncated, reason = value.reason)
  }
  private def viewFromProto(value: BatchViewPO): View = {
    known(value.unknownFields)
    bounded(value.items, MaxPageItems, "query rows"); bounded(value.batches, MaxBatches, "view batches")
    val result = View(value.jobId, phaseFrom(value.phase, jobPhases), value.inputClosed, value.accepted, value.succeeded,
      value.failed, value.held, value.active, value.activeBytes, value.batches.map(batchViewFromProto).toVector,
      value.items.map(itemViewFromProto).toVector, value.truncated, value.reason)
    viewToProto(result); result
  }

  private[persistence] def wireToProto(value: Wire, resolver: ActorRefResolver): BatchWirePO = {
    def ref(value: ActorRef[Reply]): String = {
      require(value != null, "missing batch reply actor")
      val path = resolver.toSerializationFormat(value); text(path, "reply actor"); path
    }
    val message = value match {
      case Offer(seq, id, payload, area, key, replyTo) =>
        sequence(seq); text(id, "offer item id"); text(area, "offer area"); text(key, "offer compatibility key")
        BatchWirePO.Message.Offer(BatchOfferPO(sequence = seq, itemId = id, payload = actorBytes(payload, MaxItemBytes),
          area = area, compatibilityKey = key, replyTo = ref(replyTo)))
      case CloseInput(replyTo) => BatchWirePO.Message.CloseInput(BatchCloseInputPO(replyTo = ref(replyTo)))
      case Get(replyTo, after, limit) =>
        require(after >= -1 && limit > 0 && limit <= MaxPageItems, "invalid batch query page")
        BatchWirePO.Message.Get(BatchGetPO(replyTo = ref(replyTo), afterSequence = after, limit = limit))
      case Reconcile(id, replyTo) =>
        text(id, "reconcile batch id"); BatchWirePO.Message.Reconcile(BatchReconcilePO(batchId = id, replyTo = ref(replyTo)))
      case VerifyItem(seq, area, key, replyTo) =>
        sequence(seq); text(area, "verification area"); text(key, "verification compatibility key")
        BatchWirePO.Message.VerifyItem(BatchVerifyItemPO(sequence = seq, verificationArea = area,
          compatibilityKey = key, replyTo = ref(replyTo)))
      case Accepted(seq, id, duplicate) =>
        sequence(seq); text(id, "accepted item id")
        BatchWirePO.Message.Accepted(BatchAcceptedPO(sequence = seq, itemId = id, duplicate = duplicate))
      case Backpressured(reason) => text(reason, "backpressure reason"); BatchWirePO.Message.Backpressured(BatchReasonPO(reason = reason))
      case Rejected(reason) => text(reason, "rejection reason"); BatchWirePO.Message.Rejected(BatchReasonPO(reason = reason))
      case Current(view) => BatchWirePO.Message.Current(viewToProto(view))
    }
    val result = BatchWirePO(schemaVersion = 1, message = message)
    size(result, MaxWireBytes); result
  }
  private[persistence] def wireFromProto(value: BatchWirePO, resolver: ActorRefResolver): Wire = {
    schema(value.schemaVersion); known(value.unknownFields)
    def ref(path: String): ActorRef[Reply] = { text(path, "reply actor"); resolver.resolveActorRef[Reply](path) }
    val result = value.message match {
      case BatchWirePO.Message.Offer(p) =>
        known(p.unknownFields)
        Offer(p.sequence, p.itemId, actorBytes(p.payload, MaxItemBytes), p.area, p.compatibilityKey, ref(p.replyTo))
      case BatchWirePO.Message.CloseInput(p) => known(p.unknownFields); CloseInput(ref(p.replyTo))
      case BatchWirePO.Message.Get(p) => known(p.unknownFields); Get(ref(p.replyTo), p.afterSequence, p.limit)
      case BatchWirePO.Message.Reconcile(p) => known(p.unknownFields); Reconcile(p.batchId, ref(p.replyTo))
      case BatchWirePO.Message.VerifyItem(p) =>
        known(p.unknownFields); VerifyItem(p.sequence, p.verificationArea, p.compatibilityKey, ref(p.replyTo))
      case BatchWirePO.Message.Accepted(p) => known(p.unknownFields); Accepted(p.sequence, p.itemId, p.duplicate)
      case BatchWirePO.Message.Backpressured(p) => known(p.unknownFields); Backpressured(p.reason)
      case BatchWirePO.Message.Rejected(p) => known(p.unknownFields); Rejected(p.reason)
      case BatchWirePO.Message.Current(p) => Current(viewFromProto(p))
      case BatchWirePO.Message.Empty => throw new IllegalArgumentException("missing/unknown batch wire variant")
    }
    wireToProto(result, resolver); result
  }
}
