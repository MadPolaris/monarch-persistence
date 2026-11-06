package net.imadz.monarch.persistence.operation

import net.imadz.monarch.persistence.ProtocolLimits
import net.imadz.monarch.persistence.operation.OperationProtocol._

/** Shape/identity validation shared by the actor and serializers. Connector proof
  * authorization is deliberately not part of replay or deserialization.
  */
private[persistence] object OperationValidation {
  private def optional[A](name: String, value: Option[A])(check: A => Option[String]): Option[String] =
    if (value == null) Some(s"$name must not be null") else value.flatMap(check)

  private def digest(value: String, limits: ProtocolLimits): Option[String] =
    limits.text("request digest", value).orElse {
      if (value.matches("[0-9a-f]{64}")) None else Some("request digest must be a lowercase SHA-256")
    }

  private def due(value: Option[Long]): Option[String] =
    optional("next check time", value)(n => if (n < 0L) Some("next check time must not be negative") else None)

  def request(value: Request, limits: ProtocolLimits): Option[String] = {
    if (value == null) Some("request must not be null")
    else limits.text("operation key", value.operationKey)
      .orElse(limits.text("effect key", value.effectKey))
      .orElse(limits.text("step id", value.stepId))
      .orElse(limits.text("connector id", value.connectorId))
      .orElse(limits.text("connector version", value.connectorVersion))
      .orElse(limits.text("ledger epoch", value.ledgerEpoch))
      .orElse(limits.bytes("request payload", value.payload, limits.maxRequestBytes))
      .orElse(optional("reservation id", value.reservationId)(limits.text("reservation id", _)))
  }

  def evidence(value: Evidence, limits: ProtocolLimits): Option[String] = {
    if (value == null) Some("evidence must not be null")
    else limits.text("evidence operation key", value.operationKey)
      .orElse(digest(value.requestDigest, limits))
      .orElse(limits.text("evidence ledger epoch", value.ledgerEpoch))
      .orElse(optional("external task id", value.taskId)(limits.text("external task id", _)))
      .orElse(limits.bytes("evidence payload", value.payload, limits.maxEvidenceBytes))
  }

  /** After a task id is learned, a later receipt must retain that exact id; it
    * cannot omit it, replace it, or silently switch to another external task.
    */
  def correlate(request: Request, value: Evidence, previous: Option[Evidence]): Option[String] =
    if (value == null) Some("missing evidence")
    else if (value.operationKey != request.operationKey) Some("evidence operation key mismatch")
    else if (value.requestDigest != request.fingerprint) Some("evidence request digest mismatch")
    else if (value.ledgerEpoch != request.ledgerEpoch) Some("evidence durable ledger epoch mismatch")
    else if (previous.flatMap(_.taskId).exists(id => !value.taskId.contains(id)))
      Some("evidence external task id changed or disappeared")
    else None

  def terminal(value: Terminal, limits: ProtocolLimits): Option[String] = {
    if (value == null) Some("terminal must not be null")
    else evidence(value.evidence, limits)
      .orElse(limits.bytes("terminal result", value.result, limits.maxResultBytes))
      .orElse(value match {
        case KnownFailed(code, detail, _, _) => limits.text("failure code", code)
          .orElse(limits.text("failure detail", detail, allowEmpty = true))
        case _: Succeeded => None
      })
  }

  def observation(value: WithEvidence, request: Request, previous: Option[Evidence],
      limits: ProtocolLimits): Option[String] = value match {
    case Confirmed(result) => terminal(result, limits)
      .orElse(correlate(request, result.evidence, previous))
    case other => evidence(other.evidence, limits)
      .orElse(correlate(request, other.evidence, previous))
  }

  def view(value: View, limits: ProtocolLimits): Option[String] = {
    if (value == null) Some("view must not be null")
    else limits.text("operation key", value.operationKey)
      .orElse(optional("request digest", value.requestDigest)(digest(_, limits)))
      .orElse(optional("terminal", value.terminal)(terminal(_, limits)))
      .orElse(optional("reason", value.reason)(limits.text("reason", _)))
      .orElse(due(value.nextCheckAt))
      .orElse {
        if (!Phase.values.contains(value.phase)) Some("unknown operation phase")
        else if (value.checks < 0) Some("negative query count")
        else if (value.phase == Phase.Empty && (value.requestDigest.nonEmpty || value.checks != 0))
          Some("empty view contains operation history")
        else if (value.phase != Phase.Empty && value.phase != Phase.RecoveryBlocked && value.requestDigest.isEmpty)
          Some("operation view is missing its request digest")
        else if (Set(Phase.Blocked, Phase.RecoveryBlocked).contains(value.phase) && value.reason.isEmpty)
          Some("blocked view requires a reason")
        else if (value.phase == Phase.Prepared && value.checks != 0)
          Some("prepared view contains query history")
        else if (value.phase == Phase.Ongoing && value.nextCheckAt.isEmpty)
          Some("ongoing view has no durable schedule")
        else if (value.phase == Phase.Reconciling && value.nextCheckAt.isEmpty && value.checks == 0)
          Some("unscheduled reconciliation has no recorded query")
        else if (value.nextCheckAt.nonEmpty && !Set(Phase.Ongoing, Phase.Reconciling).contains(value.phase))
          Some("phase cannot have a scheduled check")
        else value.terminal match {
          case Some(t: Succeeded) if value.phase == Phase.Succeeded =>
            viewEvidence(value, t.evidence)
          case Some(t: KnownFailed) if value.phase == Phase.Failed =>
            viewEvidence(value, t.evidence)
          case None if value.phase != Phase.Succeeded && value.phase != Phase.Failed => None
          case _ => Some("terminal and view phase disagree")
        }
      }
  }

  private def viewEvidence(view: View, proof: Evidence): Option[String] =
    if (proof.operationKey != view.operationKey || !view.requestDigest.contains(proof.requestDigest))
      Some("terminal does not identify the view request") else None

  def event(value: OperationEvent, limits: ProtocolLimits): Option[String] = {
    import OperationEvent._
    value match {
      case Prepared(r, d, p) => request(r, limits).orElse(digest(d, limits)).orElse {
        if (p == null) Some("missing operation policy")
        else if (d != r.fingerprint) Some("prepared request fingerprint mismatch") else None
      }
      case DispatchStarted(n, proof) =>
        if (n <= 0) Some("invalid submission count")
        else optional("dispatch proof", proof)(evidence(_, limits))
      case QueryStarted(n) => if (n <= 0) Some("invalid query count") else None
      case RunningObserved(proof, next) => evidence(proof, limits).orElse(due(next))
      case UnknownObserved(reason, next) => limits.text("unknown reason", reason).orElse(due(next))
      case Blocked(reason) => limits.text("blocked reason", reason)
      case Reconciled(round, allowance, next) =>
        if (round <= 0 || allowance <= 0 || next < 0L) Some("invalid reconciliation grant") else None
      case TerminalRecorded(result) => terminal(result, limits)
    }
  }

  def state(value: OperationState, limits: ProtocolLimits): Option[String] = {
    optional("request", value.request)(request(_, limits))
      .orElse(optional("request digest", value.requestDigest)(digest(_, limits)))
      .orElse(optional("policy", value.policy)(p => if (p == null) Some("missing policy") else None))
      .orElse(optional("evidence", value.evidence)(evidence(_, limits)))
      .orElse(optional("terminal", value.terminal)(terminal(_, limits)))
      .orElse(optional("reason", value.reason)(limits.text("reason", _)))
      .orElse(optional("invalid history", value.invalid)(limits.text("invalid history", _)))
      .orElse(due(value.nextCheckAt))
      .orElse {
        if (value.version != 1) Some("unsupported operation state version")
        else if (!Phase.values.contains(value.phase) || value.phase == Phase.RecoveryBlocked)
          Some("unknown persisted operation phase")
        else if (Vector(value.checks, value.remainingChecks, value.submissions, value.reconciliations).exists(_ < 0))
          Some("negative operation counter")
        else value.request match {
          case None =>
            if (value.phase != Phase.Empty || value.policy.nonEmpty || value.requestDigest.nonEmpty ||
                value.evidence.nonEmpty || value.terminal.nonEmpty || value.nextCheckAt.nonEmpty ||
                value.reason.nonEmpty || value.checks != 0 || value.remainingChecks != 0 ||
                value.submissions != 0 || value.reconciliations != 0)
              Some("empty operation contains prepared state") else None
          case Some(r) => value.policy match {
            case None => Some("prepared operation is missing its policy")
            case Some(p) =>
              if (!value.requestDigest.contains(r.fingerprint)) Some("recorded request digest mismatch")
              else if (value.phase == Phase.Empty) Some("prepared request has empty phase")
              else if (value.reconciliations > p.maxReconciliations || value.submissions > p.maxSubmissions ||
                  value.checks.toLong + value.remainingChecks > p.maxChecks.toLong +
                    value.reconciliations.toLong * p.manualCheckBudget ||
                  value.remainingChecks > (if (value.reconciliations == 0) p.maxChecks else p.manualCheckBudget))
                Some("recorded counters exceed the frozen policy")
              else if (value.phase == Phase.Prepared && (value.submissions != 0 || value.checks != 0 ||
                  value.reconciliations != 0 || value.remainingChecks != p.maxChecks || value.evidence.nonEmpty))
                Some("invalid prepared frontier")
              else if (value.phase != Phase.Prepared && value.submissions == 0)
                Some("operation has no recorded dispatch")
              else if (value.terminal.nonEmpty && value.evidence.nonEmpty)
                Some("terminal evidence must not be duplicated")
              else if (value.phase == Phase.Ongoing && value.evidence.isEmpty)
                Some("ongoing operation is missing its accepted task evidence")
              else if (value.submissions > 1 && value.checks < value.submissions - 1)
                Some("resubmission is missing a recorded authorizing query")
              else if (value.phase == Phase.Ongoing && value.nextCheckAt.isEmpty)
                Some("ongoing operation has no durable schedule")
              else if (value.nextCheckAt.nonEmpty && value.remainingChecks == 0)
                Some("scheduled query has no budget")
              else value.lastEvidence.flatMap(correlate(r, _, None)).orElse {
                view(View(r.operationKey, value.requestDigest, value.phase, value.terminal,
                  value.reason, value.nextCheckAt, value.checks), limits)
              }
          }
        }
      }
  }

  def requireValid(problem: Option[String]): Unit = problem.foreach(p => throw new IllegalArgumentException(p))
}
