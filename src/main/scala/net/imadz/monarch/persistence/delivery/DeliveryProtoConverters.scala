package net.imadz.monarch.persistence.delivery

import akka.actor.typed.{ActorRef, ActorRefResolver}
import akka.util.ByteString
import com.google.protobuf.{ByteString => ProtoBytes}
import net.imadz.monarch.persistence.{ProtocolLimits, RunProtocol, RunSubmission}
import net.imadz.monarch.persistence.delivery.DeliveryProtocol._
import net.imadz.monarch.persistence.delivery.proto._

private[persistence] object DeliveryProtoConverters {
  private def known(fields: scalapb.UnknownFieldSet): Unit =
    require(fields == scalapb.UnknownFieldSet.empty, "unknown delivery protobuf fields/alternative")
  private def schema(version: Int): Unit = require(version == 1, s"unsupported delivery schema $version")
  private def required[A](value: Option[A], name: String): A =
    value.getOrElse(throw new IllegalArgumentException(s"missing delivery $name"))
  private def bytes(value: ByteString): ProtoBytes = ProtoBytes.copyFrom(value.toArray)
  private def bytes(value: ProtoBytes): ByteString = ByteString(value.toByteArray)
  private def bounded(value: scalapb.GeneratedMessage, limits: ProtocolLimits): Unit =
    require(value.serializedSize <= limits.maxEnvelopeBytes, "delivery envelope exceeds its byte limit")

  private val phases = Map(
    Phase.Empty -> DeliveryPhasePO.PHASE_EMPTY, Phase.Accepted -> DeliveryPhasePO.PHASE_ACCEPTED,
    Phase.Starting -> DeliveryPhasePO.PHASE_STARTING, Phase.Running -> DeliveryPhasePO.PHASE_RUNNING,
    Phase.Completed -> DeliveryPhasePO.PHASE_COMPLETED, Phase.Failed -> DeliveryPhasePO.PHASE_FAILED,
    Phase.Blocked -> DeliveryPhasePO.PHASE_BLOCKED, Phase.RecoveryBlocked -> DeliveryPhasePO.PHASE_RECOVERY_BLOCKED)
  private def phaseToProto(value: String): DeliveryPhasePO =
    phases.getOrElse(value, throw new IllegalArgumentException("unknown delivery phase"))
  private def phaseFromProto(value: DeliveryPhasePO): String =
    phases.collectFirst { case (name, phase) if phase == value => name }
      .getOrElse(throw new IllegalArgumentException("unknown/unspecified delivery phase"))

  private def actionToProto(value: Option[String]): DeliveryActionPO = value match {
    case None => DeliveryActionPO.ACTION_NONE
    case Some(DeliveryState.Ensure) => DeliveryActionPO.ACTION_ENSURE
    case Some(DeliveryState.Query) => DeliveryActionPO.ACTION_QUERY
    case _ => throw new IllegalArgumentException("unknown delivery action")
  }
  private def actionFromProto(value: DeliveryActionPO): Option[String] = value match {
    case DeliveryActionPO.ACTION_NONE => None
    case DeliveryActionPO.ACTION_ENSURE => Some(DeliveryState.Ensure)
    case DeliveryActionPO.ACTION_QUERY => Some(DeliveryState.Query)
    case _ => throw new IllegalArgumentException("unknown delivery action")
  }

  private def submissionToProto(value: RunSubmission): DeliverySubmissionPO = DeliverySubmissionPO(
    runId = value.runId, definitionId = value.definitionId, definitionVersion = value.definitionVersion,
    codecVersion = value.codecVersion, initialState = bytes(value.initialState))
  private def submissionFromProto(value: DeliverySubmissionPO): RunSubmission = {
    known(value.unknownFields)
    RunSubmission(value.runId, value.definitionId, value.definitionVersion, value.codecVersion, bytes(value.initialState))
  }
  def requestToProto(value: Request): DeliveryRequestPO = DeliveryRequestPO(deliveryId = value.deliveryId,
    sourceRunId = value.sourceRunId, target = value.target, child = Some(submissionToProto(value.child)), payload = bytes(value.payload))
  private def requestFromProto(value: DeliveryRequestPO): Request = {
    known(value.unknownFields)
    Request(value.deliveryId, value.sourceRunId, value.target,
      submissionFromProto(required(value.child, "child submission")), bytes(value.payload))
  }

  def identityToProto(value: RunProtocol.Identity): DeliveryChildIdentityPO = DeliveryChildIdentityPO(
    runId = value.runId, definitionId = value.definitionId, definitionVersion = value.definitionVersion,
    codecVersion = value.codecVersion, initialDigest = value.initialDigest, planDigest = value.planDigest)
  private def identityFromProto(value: DeliveryChildIdentityPO): RunProtocol.Identity = {
    known(value.unknownFields)
    RunProtocol.Identity(value.runId, value.definitionId, value.definitionVersion, value.codecVersion,
      value.initialDigest, value.planDigest)
  }

  private def limitsToProto(value: ProtocolLimits): DeliveryLimitsPO = DeliveryLimitsPO(
    maxRequestBytes = value.maxRequestBytes, maxStateBytes = value.maxStateBytes, maxResultBytes = value.maxResultBytes,
    maxEvidenceBytes = value.maxEvidenceBytes, maxEnvelopeBytes = value.maxEnvelopeBytes,
    maxStringBytes = value.maxStringBytes, maxPlanSteps = value.maxPlanSteps, maxMetadataEntries = value.maxMetadataEntries)
  private def limitsFromProto(value: DeliveryLimitsPO): ProtocolLimits = {
    known(value.unknownFields)
    ProtocolLimits(value.maxRequestBytes, value.maxStateBytes, value.maxResultBytes, value.maxEvidenceBytes,
      value.maxEnvelopeBytes, value.maxStringBytes, value.maxPlanSteps, value.maxMetadataEntries)
  }
  private def policyToProto(value: DeliveryPolicy): DeliveryPolicyPO = DeliveryPolicyPO(
    queryTimeoutMillis = value.queryTimeoutMillis, pollIntervalMillis = value.pollIntervalMillis,
    maxPolls = value.maxPolls, maxReconciliations = value.maxReconciliations, limits = Some(limitsToProto(value.limits)))
  private def policyFromProto(value: DeliveryPolicyPO): DeliveryPolicy = {
    known(value.unknownFields)
    DeliveryPolicy(value.queryTimeoutMillis, value.pollIntervalMillis, value.maxPolls, value.maxReconciliations,
      limitsFromProto(required(value.limits, "frozen limits")))
  }

  def eventToProto(value: DeliveryEvent): DeliveryEventPO = {
    import DeliveryEvent._
    val event = value match {
      case Accepted(request, digest, policy) => DeliveryEventPO.Event.Accepted(DeliveryAcceptedPO(
        request = Some(requestToProto(request)), requestDigest = digest, policy = Some(policyToProto(policy))))
      case ChildStartIntent(attempt) => DeliveryEventPO.Event.ChildStartIntent(DeliveryChildStartIntentPO(attempt = attempt))
      case ChildQueryIntent(poll) => DeliveryEventPO.Event.ChildQueryIntent(DeliveryChildQueryIntentPO(poll = poll))
      case ChildRunning(identity, next) => DeliveryEventPO.Event.ChildRunning(DeliveryChildRunningPO(
        identity = Some(identityToProto(identity)), nextPollAt = next))
      case ChildTerminalRecorded(identity, phase, result, reason) =>
        DeliveryEventPO.Event.ChildTerminalRecorded(DeliveryChildTerminalRecordedPO(
          identity = Some(identityToProto(identity)), phase = phaseToProto(phase), result = bytes(result), reason = reason))
      case Blocked(reason) => DeliveryEventPO.Event.Blocked(DeliveryBlockedPO(reason = reason))
      case Reconciled(round, due) => DeliveryEventPO.Event.Reconciled(DeliveryReconciledPO(round = round, nextPollAt = due))
    }
    DeliveryEventPO(schemaVersion = 1, event = event)
  }

  def eventFromProto(value: DeliveryEventPO, limits: ProtocolLimits = ProtocolLimits()): DeliveryEvent = {
    import DeliveryEvent._
    schema(value.schemaVersion); known(value.unknownFields); bounded(value, limits)
    val event: DeliveryEvent = value.event match {
      case DeliveryEventPO.Event.Accepted(p) =>
        known(p.unknownFields)
        Accepted(requestFromProto(required(p.request, "accepted request")), p.requestDigest,
          policyFromProto(required(p.policy, "accepted policy")))
      case DeliveryEventPO.Event.ChildStartIntent(p) => known(p.unknownFields); ChildStartIntent(p.attempt)
      case DeliveryEventPO.Event.ChildQueryIntent(p) => known(p.unknownFields); ChildQueryIntent(p.poll)
      case DeliveryEventPO.Event.ChildRunning(p) =>
        known(p.unknownFields); ChildRunning(identityFromProto(required(p.identity, "running child identity")), p.nextPollAt)
      case DeliveryEventPO.Event.ChildTerminalRecorded(p) =>
        known(p.unknownFields)
        ChildTerminalRecorded(identityFromProto(required(p.identity, "terminal child identity")),
          phaseFromProto(p.phase), bytes(p.result), p.reason)
      case DeliveryEventPO.Event.Blocked(p) => known(p.unknownFields); Blocked(p.reason)
      case DeliveryEventPO.Event.Reconciled(p) => known(p.unknownFields); Reconciled(p.round, p.nextPollAt)
      case DeliveryEventPO.Event.Empty => throw new IllegalArgumentException("missing/unknown delivery event variant")
    }
    DeliveryValidation.requireValid(DeliveryValidation.event(event, limits))
    event
  }

  def stateToProto(value: DeliveryState): DeliveryStatePO = DeliveryStatePO(
    schemaVersion = 1, stateVersion = value.version, request = value.request.map(requestToProto),
    requestDigest = value.requestDigest, policy = value.policy.map(policyToProto), phase = phaseToProto(value.phase),
    childIdentity = value.childIdentity.map(identityToProto), result = value.result.map(bytes), reason = value.reason,
    nextAction = actionToProto(value.nextAction), nextPollAt = value.nextPollAt, starts = value.starts,
    polls = value.polls, remainingPolls = value.remainingPolls, reconciliations = value.reconciliations, invalid = value.invalid)

  def stateFromProto(value: DeliveryStatePO, limits: ProtocolLimits = ProtocolLimits()): DeliveryState = {
    schema(value.schemaVersion); known(value.unknownFields); bounded(value, limits)
    val state = DeliveryState(request = value.request.map(requestFromProto), requestDigest = value.requestDigest,
      policy = value.policy.map(policyFromProto), phase = phaseFromProto(value.phase),
      childIdentity = value.childIdentity.map(identityFromProto), result = value.result.map(bytes), reason = value.reason,
      nextAction = actionFromProto(value.nextAction), nextPollAt = value.nextPollAt, starts = value.starts,
      polls = value.polls, remainingPolls = value.remainingPolls, reconciliations = value.reconciliations,
      invalid = value.invalid, version = value.stateVersion)
    DeliveryValidation.requireValid(DeliveryValidation.state(state, limits))
    state
  }

  def viewToProto(value: View): DeliveryViewPO = DeliveryViewPO(deliveryId = value.deliveryId,
    requestDigest = value.requestDigest, childRunId = value.childRunId, phase = phaseToProto(value.phase),
    result = value.result.map(bytes), reason = value.reason, reconciling = value.reconciling)
  private def viewFromProto(value: DeliveryViewPO, limits: ProtocolLimits): View = {
    known(value.unknownFields)
    val view = View(value.deliveryId, value.requestDigest, value.childRunId, phaseFromProto(value.phase),
      value.result.map(bytes), value.reason, value.reconciling)
    DeliveryValidation.requireValid(DeliveryValidation.view(view, limits))
    view
  }

  def wireToProto(value: Wire, resolver: ActorRefResolver, limits: ProtocolLimits): DeliveryWirePO = {
    def ref(value: ActorRef[Reply]): String = {
      require(value != null, "missing delivery reply actor")
      val path = resolver.toSerializationFormat(value)
      DeliveryValidation.requireValid(limits.text("delivery reply actor", path))
      path
    }
    val message = value match {
      case Deliver(request, replyTo) =>
        DeliveryValidation.requireValid(DeliveryValidation.request(request, limits))
        DeliveryWirePO.Message.Deliver(DeliveryDeliverPO(request = Some(requestToProto(request)), replyTo = ref(replyTo)))
      case Get(id, replyTo) =>
        DeliveryValidation.requireValid(limits.text("delivery id", id))
        DeliveryWirePO.Message.Get(DeliveryGetPO(deliveryId = id, replyTo = ref(replyTo)))
      case Reconcile(id, replyTo) =>
        DeliveryValidation.requireValid(limits.text("delivery id", id))
        DeliveryWirePO.Message.Reconcile(DeliveryReconcilePO(deliveryId = id, replyTo = ref(replyTo)))
      case Current(view) =>
        DeliveryValidation.requireValid(DeliveryValidation.view(view, limits))
        DeliveryWirePO.Message.Current(DeliveryCurrentPO(view = Some(viewToProto(view))))
      case Rejected(id, reason) =>
        DeliveryValidation.requireValid(limits.text("delivery id", id).orElse(limits.text("delivery reason", reason)))
        DeliveryWirePO.Message.Rejected(DeliveryRejectedPO(deliveryId = id, reason = reason))
    }
    val proto = DeliveryWirePO(schemaVersion = 1, message = message)
    bounded(proto, limits)
    proto
  }

  def wireFromProto(value: DeliveryWirePO, resolver: ActorRefResolver, limits: ProtocolLimits): Wire = {
    schema(value.schemaVersion); known(value.unknownFields); bounded(value, limits)
    def ref(path: String): ActorRef[Reply] = {
      DeliveryValidation.requireValid(limits.text("delivery reply actor", path))
      resolver.resolveActorRef[Reply](path)
    }
    value.message match {
      case DeliveryWirePO.Message.Deliver(p) =>
        known(p.unknownFields)
        val request = requestFromProto(required(p.request, "deliver request"))
        DeliveryValidation.requireValid(DeliveryValidation.request(request, limits))
        Deliver(request, ref(p.replyTo))
      case DeliveryWirePO.Message.Get(p) =>
        known(p.unknownFields); DeliveryValidation.requireValid(limits.text("delivery id", p.deliveryId))
        Get(p.deliveryId, ref(p.replyTo))
      case DeliveryWirePO.Message.Reconcile(p) =>
        known(p.unknownFields); DeliveryValidation.requireValid(limits.text("delivery id", p.deliveryId))
        Reconcile(p.deliveryId, ref(p.replyTo))
      case DeliveryWirePO.Message.Current(p) =>
        known(p.unknownFields); Current(viewFromProto(required(p.view, "current view"), limits))
      case DeliveryWirePO.Message.Rejected(p) =>
        known(p.unknownFields)
        DeliveryValidation.requireValid(limits.text("delivery id", p.deliveryId).orElse(limits.text("delivery reason", p.reason)))
        Rejected(p.deliveryId, p.reason)
      case DeliveryWirePO.Message.Empty => throw new IllegalArgumentException("missing/unknown delivery wire variant")
    }
  }
}
