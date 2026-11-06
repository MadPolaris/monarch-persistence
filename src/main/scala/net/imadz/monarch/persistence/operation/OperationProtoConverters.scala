package net.imadz.monarch.persistence.operation

import akka.actor.typed.{ActorRef, ActorRefResolver}
import akka.util.ByteString
import com.google.protobuf.{ByteString => ProtoBytes}
import net.imadz.monarch.persistence.ProtocolLimits
import net.imadz.monarch.persistence.operation.OperationProtocol._
import net.imadz.monarch.persistence.operation.proto._

private[persistence] object OperationProtoConverters {
  private def known(fields: scalapb.UnknownFieldSet): Unit =
    require(fields == scalapb.UnknownFieldSet.empty, "unknown operation protobuf fields/alternative")
  private def schema(version: Int): Unit = require(version == 1, s"unsupported operation schema $version")
  private def required[A](value: Option[A], name: String): A =
    value.getOrElse(throw new IllegalArgumentException(s"missing operation $name"))
  private def bytes(value: ByteString): ProtoBytes = ProtoBytes.copyFrom(value.toArray)
  private def bytes(value: ProtoBytes): ByteString = ByteString(value.toByteArray)

  private val phases = Map(
    Phase.Empty -> OperationPhasePO.PHASE_EMPTY,
    Phase.Prepared -> OperationPhasePO.PHASE_PREPARED,
    Phase.Dispatching -> OperationPhasePO.PHASE_DISPATCHING,
    Phase.Ongoing -> OperationPhasePO.PHASE_ONGOING,
    Phase.Reconciling -> OperationPhasePO.PHASE_RECONCILING,
    Phase.Blocked -> OperationPhasePO.PHASE_BLOCKED,
    Phase.Succeeded -> OperationPhasePO.PHASE_SUCCEEDED,
    Phase.Failed -> OperationPhasePO.PHASE_FAILED,
    Phase.RecoveryBlocked -> OperationPhasePO.PHASE_RECOVERY_BLOCKED)
  private def phaseToProto(value: String): OperationPhasePO =
    phases.getOrElse(value, throw new IllegalArgumentException("unknown operation phase"))
  private def phaseFromProto(value: OperationPhasePO): String =
    phases.collectFirst { case (name, phase) if phase == value => name }
      .getOrElse(throw new IllegalArgumentException("unknown/unspecified operation phase"))

  def requestToProto(value: Request): OperationRequestPO = OperationRequestPO(
    operationKey = value.operationKey, effectKey = value.effectKey, stepId = value.stepId,
    connectorId = value.connectorId, connectorVersion = value.connectorVersion,
    ledgerEpoch = value.ledgerEpoch, payload = bytes(value.payload), reservationId = value.reservationId)

  private def requestFromProto(value: OperationRequestPO): Request = {
    known(value.unknownFields)
    Request(value.operationKey, value.effectKey, value.stepId, value.connectorId,
      value.connectorVersion, value.ledgerEpoch, bytes(value.payload), value.reservationId)
  }

  def evidenceToProto(value: Evidence): OperationEvidencePO = OperationEvidencePO(
    operationKey = value.operationKey, requestDigest = value.requestDigest,
    ledgerEpoch = value.ledgerEpoch, taskId = value.taskId, payload = bytes(value.payload))

  private def evidenceFromProto(value: OperationEvidencePO): Evidence = {
    known(value.unknownFields)
    Evidence(value.operationKey, value.requestDigest, value.ledgerEpoch, value.taskId, bytes(value.payload))
  }

  def terminalToProto(value: Terminal): OperationTerminalPO = {
    val terminal = value match {
      case Succeeded(result, evidence) => OperationTerminalPO.Terminal.Succeeded(
        OperationSucceededPO(result = bytes(result), evidence = Some(evidenceToProto(evidence))))
      case KnownFailed(code, detail, result, evidence) => OperationTerminalPO.Terminal.Failed(
        OperationKnownFailedPO(code = code, detail = detail, result = bytes(result),
          evidence = Some(evidenceToProto(evidence))))
    }
    OperationTerminalPO(terminal = terminal)
  }

  private def terminalFromProto(value: OperationTerminalPO): Terminal = {
    known(value.unknownFields)
    value.terminal match {
      case OperationTerminalPO.Terminal.Succeeded(p) =>
        known(p.unknownFields)
        Succeeded(bytes(p.result), evidenceFromProto(required(p.evidence, "success evidence")))
      case OperationTerminalPO.Terminal.Failed(p) =>
        known(p.unknownFields)
        KnownFailed(p.code, p.detail, bytes(p.result), evidenceFromProto(required(p.evidence, "failure evidence")))
      case OperationTerminalPO.Terminal.Empty => throw new IllegalArgumentException("missing/unknown terminal variant")
    }
  }

  private def policyToProto(value: OperationPolicy): OperationPolicyPO = OperationPolicyPO(
    ioTimeoutMillis = value.ioTimeoutMillis, checkIntervalMillis = value.checkIntervalMillis,
    maxChecks = value.maxChecks, manualCheckBudget = value.manualCheckBudget,
    maxReconciliations = value.maxReconciliations, maxSubmissions = value.maxSubmissions)

  private def policyFromProto(value: OperationPolicyPO): OperationPolicy = {
    known(value.unknownFields)
    OperationPolicy(value.ioTimeoutMillis, value.checkIntervalMillis, value.maxChecks,
      value.manualCheckBudget, value.maxReconciliations, value.maxSubmissions)
  }

  def eventToProto(value: OperationEvent): OperationEventPO = {
    import OperationEvent._
    val event = value match {
      case Prepared(request, digest, policy) => OperationEventPO.Event.Prepared(OperationPreparedPO(
        request = Some(requestToProto(request)), requestDigest = digest, policy = Some(policyToProto(policy))))
      case DispatchStarted(submission, proof) => OperationEventPO.Event.DispatchStarted(
        OperationDispatchStartedPO(submission = submission, authorization = proof.map(evidenceToProto)))
      case QueryStarted(check) => OperationEventPO.Event.QueryStarted(OperationQueryStartedPO(check = check))
      case RunningObserved(evidence, next) => OperationEventPO.Event.RunningObserved(
        OperationRunningObservedPO(evidence = Some(evidenceToProto(evidence)), nextCheckAt = next))
      case UnknownObserved(reason, next) => OperationEventPO.Event.UnknownObserved(
        OperationUnknownObservedPO(reason = reason, nextCheckAt = next))
      case Blocked(reason) => OperationEventPO.Event.Blocked(OperationBlockedPO(reason = reason))
      case Reconciled(round, allowance, due) => OperationEventPO.Event.Reconciled(
        OperationReconciledPO(round = round, allowance = allowance, nextCheckAt = due))
      case TerminalRecorded(result) => OperationEventPO.Event.TerminalRecorded(
        OperationTerminalRecordedPO(terminal = Some(terminalToProto(result))))
    }
    OperationEventPO(schemaVersion = 1, event = event)
  }

  def eventFromProto(value: OperationEventPO, limits: ProtocolLimits = ProtocolLimits()): OperationEvent = {
    import OperationEvent._
    schema(value.schemaVersion); known(value.unknownFields)
    val event: OperationEvent = value.event match {
      case OperationEventPO.Event.Prepared(p) =>
        known(p.unknownFields)
        Prepared(requestFromProto(required(p.request, "prepared request")), p.requestDigest,
          policyFromProto(required(p.policy, "prepared policy")))
      case OperationEventPO.Event.DispatchStarted(p) =>
        known(p.unknownFields); DispatchStarted(p.submission, p.authorization.map(evidenceFromProto))
      case OperationEventPO.Event.QueryStarted(p) =>
        known(p.unknownFields); QueryStarted(p.check)
      case OperationEventPO.Event.RunningObserved(p) =>
        known(p.unknownFields)
        RunningObserved(evidenceFromProto(required(p.evidence, "running evidence")), p.nextCheckAt)
      case OperationEventPO.Event.UnknownObserved(p) =>
        known(p.unknownFields); UnknownObserved(p.reason, p.nextCheckAt)
      case OperationEventPO.Event.Blocked(p) =>
        known(p.unknownFields); Blocked(p.reason)
      case OperationEventPO.Event.Reconciled(p) =>
        known(p.unknownFields); Reconciled(p.round, p.allowance, p.nextCheckAt)
      case OperationEventPO.Event.TerminalRecorded(p) =>
        known(p.unknownFields); TerminalRecorded(terminalFromProto(required(p.terminal, "terminal")))
      case OperationEventPO.Event.Empty => throw new IllegalArgumentException("missing/unknown operation event variant")
    }
    OperationValidation.requireValid(OperationValidation.event(event, limits))
    event
  }

  def stateToProto(value: OperationState): OperationStatePO = OperationStatePO(
    schemaVersion = 1, stateVersion = value.version,
    request = value.request.map(requestToProto), requestDigest = value.requestDigest,
    policy = value.policy.map(policyToProto), phase = phaseToProto(value.phase),
    evidence = value.evidence.map(evidenceToProto), terminal = value.terminal.map(terminalToProto),
    reason = value.reason, nextCheckAt = value.nextCheckAt,
    checks = value.checks, remainingChecks = value.remainingChecks, submissions = value.submissions,
    reconciliations = value.reconciliations, invalid = value.invalid)

  def stateFromProto(value: OperationStatePO, limits: ProtocolLimits = ProtocolLimits()): OperationState = {
    schema(value.schemaVersion); known(value.unknownFields)
    val state = OperationState(request = value.request.map(requestFromProto),
      requestDigest = value.requestDigest, policy = value.policy.map(policyFromProto),
      phase = phaseFromProto(value.phase), evidence = value.evidence.map(evidenceFromProto),
      terminal = value.terminal.map(terminalFromProto), reason = value.reason,
      nextCheckAt = value.nextCheckAt, checks = value.checks, remainingChecks = value.remainingChecks,
      submissions = value.submissions, reconciliations = value.reconciliations,
      invalid = value.invalid, version = value.stateVersion)
    OperationValidation.requireValid(OperationValidation.state(state, limits))
    state
  }

  def viewToProto(value: View): OperationViewPO = OperationViewPO(
    operationKey = value.operationKey, requestDigest = value.requestDigest, phase = phaseToProto(value.phase),
    terminal = value.terminal.map(terminalToProto), reason = value.reason,
    nextCheckAt = value.nextCheckAt, checks = value.checks)

  private def viewFromProto(value: OperationViewPO, limits: ProtocolLimits): View = {
    known(value.unknownFields)
    val view = View(value.operationKey, value.requestDigest, phaseFromProto(value.phase),
      value.terminal.map(terminalFromProto), value.reason, value.nextCheckAt, value.checks)
    OperationValidation.requireValid(OperationValidation.view(view, limits))
    view
  }

  def wireToProto(value: Wire, resolver: ActorRefResolver, limits: ProtocolLimits): OperationWirePO = {
    def ref(value: ActorRef[Reply]): String = {
      require(value != null, "missing reply actor")
      val path = resolver.toSerializationFormat(value)
      OperationValidation.requireValid(limits.text("reply actor", path))
      path
    }
    val message = value match {
      case Ensure(request, replyTo) =>
        OperationValidation.requireValid(OperationValidation.request(request, limits))
        OperationWirePO.Message.Ensure(OperationEnsurePO(request = Some(requestToProto(request)), replyTo = ref(replyTo)))
      case Get(key, replyTo) =>
        OperationValidation.requireValid(limits.text("operation key", key))
        OperationWirePO.Message.Get(OperationGetPO(operationKey = key, replyTo = ref(replyTo)))
      case Reconcile(key, replyTo) =>
        OperationValidation.requireValid(limits.text("operation key", key))
        OperationWirePO.Message.Reconcile(OperationReconcilePO(operationKey = key, replyTo = ref(replyTo)))
      case Current(view) =>
        OperationValidation.requireValid(OperationValidation.view(view, limits))
        OperationWirePO.Message.Current(OperationCurrentPO(view = Some(viewToProto(view))))
      case Rejected(key, reason) =>
        OperationValidation.requireValid(limits.text("operation key", key).orElse(limits.text("reason", reason)))
        OperationWirePO.Message.Rejected(OperationRejectedPO(operationKey = key, reason = reason))
    }
    OperationWirePO(schemaVersion = 1, message = message)
  }

  def wireFromProto(value: OperationWirePO, resolver: ActorRefResolver, limits: ProtocolLimits): Wire = {
    schema(value.schemaVersion); known(value.unknownFields)
    def ref(path: String): ActorRef[Reply] = {
      OperationValidation.requireValid(limits.text("reply actor", path))
      resolver.resolveActorRef[Reply](path)
    }
    value.message match {
      case OperationWirePO.Message.Ensure(p) =>
        known(p.unknownFields)
        val request = requestFromProto(required(p.request, "ensure request"))
        OperationValidation.requireValid(OperationValidation.request(request, limits))
        Ensure(request, ref(p.replyTo))
      case OperationWirePO.Message.Get(p) =>
        known(p.unknownFields)
        OperationValidation.requireValid(limits.text("operation key", p.operationKey))
        Get(p.operationKey, ref(p.replyTo))
      case OperationWirePO.Message.Reconcile(p) =>
        known(p.unknownFields)
        OperationValidation.requireValid(limits.text("operation key", p.operationKey))
        Reconcile(p.operationKey, ref(p.replyTo))
      case OperationWirePO.Message.Current(p) =>
        known(p.unknownFields); Current(viewFromProto(required(p.view, "current view"), limits))
      case OperationWirePO.Message.Rejected(p) =>
        known(p.unknownFields)
        OperationValidation.requireValid(limits.text("operation key", p.operationKey).orElse(limits.text("reason", p.reason)))
        Rejected(p.operationKey, p.reason)
      case OperationWirePO.Message.Empty => throw new IllegalArgumentException("missing/unknown operation wire variant")
    }
  }
}
