package net.imadz.monarch.persistence.operation

import net.imadz.monarch.persistence.ProtocolLimits
import net.imadz.monarch.persistence.operation.OperationProtocol._

private[persistence] sealed trait OperationEvent
private[persistence] object OperationEvent {
  final case class Prepared(request: Request, requestDigest: String, policy: OperationPolicy) extends OperationEvent
  final case class DispatchStarted(submission: Int, authorization: Option[Evidence]) extends OperationEvent
  final case class QueryStarted(check: Int) extends OperationEvent
  final case class RunningObserved(evidence: Evidence, nextCheckAt: Option[Long]) extends OperationEvent
  final case class UnknownObserved(reason: String, nextCheckAt: Option[Long]) extends OperationEvent
  final case class Blocked(reason: String) extends OperationEvent
  final case class Reconciled(round: Int, allowance: Int, nextCheckAt: Long) extends OperationEvent
  final case class TerminalRecorded(terminal: Terminal) extends OperationEvent
}

/** Replay is pure: no ports, clocks, actor refs, timers or callbacks live here.
  * checks is lifetime query count; remainingChecks is the current finite grant.
  * Terminal evidence is stored only inside terminal, not duplicated in evidence.
  */
private[persistence] final case class OperationState(
    request: Option[Request] = None,
    requestDigest: Option[String] = None,
    policy: Option[OperationPolicy] = None,
    phase: String = Phase.Empty,
    evidence: Option[Evidence] = None,
    terminal: Option[Terminal] = None,
    reason: Option[String] = None,
    nextCheckAt: Option[Long] = None,
    checks: Int = 0,
    remainingChecks: Int = 0,
    submissions: Int = 0,
    reconciliations: Int = 0,
    invalid: Option[String] = None,
    version: Int = 1
) {
  def lastEvidence: Option[Evidence] = evidence.orElse(terminal.map(_.evidence))
}

private[persistence] object OperationState {
  import OperationEvent._
  val Exhausted: String = "query budget exhausted; outcome remains unconfirmed"

  def evolve(state: OperationState, event: OperationEvent): OperationState = {
    def bad(message: String): OperationState = state.copy(invalid = state.invalid.orElse(Some(message)))
    def sameEvidence(e: Evidence): Boolean = state.request.exists { request =>
      OperationValidation.correlate(request, e, state.lastEvidence).isEmpty
    }
    def validDue(next: Option[Long]): Boolean =
      next.forall(_ >= 0L) && next.isDefined == (state.remainingChecks > 0)
    def mayObserve: Boolean = state.request.nonEmpty && state.submissions > 0 &&
      Set(Phase.Dispatching, Phase.Ongoing, Phase.Reconciling, Phase.Blocked).contains(state.phase)

    if (state.invalid.nonEmpty) state
    else if (state.terminal.nonEmpty) event match {
      case TerminalRecorded(value) if state.terminal.contains(value) => state
      case _ => bad("conflicting event after a committed terminal") // Never erase the terminal.
    }
    else event match {
      case Prepared(request, digest, policy) if state.phase == Phase.Empty && state.request.isEmpty =>
        if (digest != request.fingerprint) bad("prepared request fingerprint mismatch")
        else OperationState(request = Some(request), requestDigest = Some(digest), policy = Some(policy),
          phase = Phase.Prepared, remainingChecks = policy.maxChecks)

      case DispatchStarted(submission, proof) if state.policy.nonEmpty =>
        val first = state.phase == Phase.Prepared && state.submissions == 0 && proof.isEmpty
        val authorized = Set(Phase.Reconciling, Phase.Blocked).contains(state.phase) &&
          mayObserve && proof.exists(sameEvidence)
        if (submission != state.submissions + 1 || submission > state.policy.get.maxSubmissions ||
            submission.toLong > state.checks.toLong + 1L || (!first && !authorized))
          bad("invalid dispatch transition or submission budget")
        else state.copy(phase = Phase.Dispatching, submissions = submission,
          evidence = proof.orElse(state.evidence), reason = None, nextCheckAt = None)

      case QueryStarted(check) if state.policy.nonEmpty =>
        if (!Set(Phase.Dispatching, Phase.Ongoing, Phase.Reconciling).contains(state.phase) ||
            state.submissions == 0 || state.remainingChecks <= 0 || check != state.checks + 1)
          bad("invalid query transition or exhausted budget")
        else state.copy(phase = Phase.Reconciling, checks = check,
          remainingChecks = state.remainingChecks - 1, nextCheckAt = None, reason = None)

      case RunningObserved(proof, next) if mayObserve =>
        if (!sameEvidence(proof) || !validDue(next)) bad("invalid running evidence or schedule")
        else state.copy(phase = if (next.isDefined) Phase.Ongoing else Phase.Blocked,
          evidence = Some(proof), nextCheckAt = next,
          reason = if (next.isDefined) None else Some(Exhausted))

      case UnknownObserved(reason, next) if mayObserve =>
        if (!validDue(next)) bad("invalid unknown schedule")
        else state.copy(phase = if (next.isDefined) Phase.Reconciling else Phase.Blocked,
          reason = Some(reason), nextCheckAt = next)

      case Blocked(reason) if state.request.nonEmpty =>
        state.copy(phase = Phase.Blocked, reason = Some(reason), nextCheckAt = None)

      case Reconciled(round, allowance, due) if state.phase == Phase.Blocked && state.policy.nonEmpty =>
        val policy = state.policy.get
        if (round != state.reconciliations + 1 || round > policy.maxReconciliations ||
            allowance != policy.manualCheckBudget || due < 0L) bad("invalid manual query grant")
        else state.copy(phase = Phase.Reconciling, reconciliations = round,
          remainingChecks = allowance, nextCheckAt = Some(due), reason = None)

      case TerminalRecorded(value) if mayObserve =>
        if (!sameEvidence(value.evidence)) bad("terminal evidence does not identify the prepared operation")
        else state.copy(phase = value match {
          case _: Succeeded => Phase.Succeeded
          case _: KnownFailed => Phase.Failed
        }, terminal = Some(value), evidence = None, reason = None, nextCheckAt = None)

      case _ => bad("event is not valid at the recorded operation frontier")
    }
  }

  def recoveryProblem(state: OperationState, operationKey: String, connectorId: String,
      connectorVersion: String, limits: ProtocolLimits): Option[String] =
    state.invalid.orElse(OperationValidation.state(state, limits)).orElse {
      state.request.flatMap { request =>
        if (request.operationKey != operationKey) Some("recorded operation key differs from this entity")
        else if (request.connectorId != connectorId) Some("recorded connector id is unavailable")
        else if (request.connectorVersion != connectorVersion) Some("recorded connector version is unavailable")
        else None
      }
    }
}
