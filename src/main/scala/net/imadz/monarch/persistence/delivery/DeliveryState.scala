package net.imadz.monarch.persistence.delivery

import akka.util.ByteString
import net.imadz.monarch.persistence.{ProtocolLimits, RunProtocol}
import net.imadz.monarch.persistence.delivery.DeliveryProtocol._

private[persistence] sealed trait DeliveryEvent
private[persistence] object DeliveryEvent {
  final case class Accepted(request: Request, requestDigest: String, policy: DeliveryPolicy) extends DeliveryEvent
  final case class ChildStartIntent(attempt: Int) extends DeliveryEvent
  final case class ChildQueryIntent(poll: Int) extends DeliveryEvent
  final case class ChildRunning(identity: RunProtocol.Identity, nextPollAt: Option[Long]) extends DeliveryEvent
  final case class ChildTerminalRecorded(identity: RunProtocol.Identity, phase: String,
      result: ByteString, reason: Option[String]) extends DeliveryEvent
  final case class Blocked(reason: String) extends DeliveryEvent
  final case class Reconciled(round: Int, nextPollAt: Long) extends DeliveryEvent
}

/** Pure, constant-size replay state. Only terminal child bytes are retained; no
  * child cursors, running snapshots, observers or local waiters are journaled.
  * nextAction describes recovery work, not a claim that a Future is still live.
  */
private[persistence] final case class DeliveryState(
    request: Option[Request] = None,
    requestDigest: Option[String] = None,
    policy: Option[DeliveryPolicy] = None,
    phase: String = Phase.Empty,
    childIdentity: Option[RunProtocol.Identity] = None,
    result: Option[ByteString] = None,
    reason: Option[String] = None,
    nextAction: Option[String] = None,
    nextPollAt: Option[Long] = None,
    starts: Int = 0,
    polls: Int = 0,
    remainingPolls: Int = 0,
    reconciliations: Int = 0,
    invalid: Option[String] = None,
    version: Int = 1
) {
  def terminal: Boolean = Phase.terminal.contains(phase)
}

private[persistence] object DeliveryState {
  import DeliveryEvent._
  val Ensure: String = "Ensure"
  val Query: String = "Query"
  val Exhausted: String = "child query budget exhausted; completion remains unconfirmed"
  val StartsExhausted: String = "same-child start budget exhausted; reconciliation required"

  def evolve(state: DeliveryState, event: DeliveryEvent): DeliveryState = {
    def bad(message: String): DeliveryState = state.copy(invalid = state.invalid.orElse(Some(message)))
    def correlated(identity: RunProtocol.Identity): Boolean = state.request.exists { request =>
      DeliveryValidation.correlate(request.child, identity, state.childIdentity).isEmpty
    }
    def canObserve: Boolean = state.request.nonEmpty && state.starts > 0 &&
      Set(Phase.Starting, Phase.Running, Phase.Blocked).contains(state.phase)

    if (state.invalid.nonEmpty) state
    else if (state.version != 1) bad("unsupported delivery state version")
    else if (state.terminal) event match {
      case ChildTerminalRecorded(id, phase, bytes, reason)
          if state.childIdentity.contains(id) && state.phase == phase && state.result.contains(bytes) && state.reason == reason => state
      case _ => bad("conflicting event after a committed child terminal")
    }
    else DeliveryValidation.event(event, state.policy.map(_.limits).getOrElse(ProtocolLimits())) match {
      case Some(reason) => bad(reason)
      case None => event match {
        case Accepted(request, digest, policy) if state.phase == Phase.Empty && state.request.isEmpty =>
          DeliveryState(request = Some(request), requestDigest = Some(digest), policy = Some(policy),
            phase = Phase.Accepted, nextAction = Some(Ensure), remainingPolls = policy.maxPolls)

        case ChildStartIntent(attempt) if state.policy.nonEmpty =>
          val initialOrRecovery = Set(Phase.Accepted, Phase.Starting).contains(state.phase) && state.nextAction.contains(Ensure)
          val queriedMissing = canObserve && state.nextAction.contains(Query) && state.nextPollAt.isEmpty && state.polls > 0
          if ((!initialOrRecovery && !queriedMissing) || attempt != state.starts + 1 || attempt > state.policy.get.maxStarts)
            bad("invalid child start intent or exhausted start budget")
          else state.copy(phase = Phase.Starting, starts = attempt, nextAction = Some(Ensure), nextPollAt = None, reason = None)

        case ChildQueryIntent(poll) if state.policy.nonEmpty =>
          if (!Set(Phase.Running, Phase.Blocked).contains(state.phase) || !state.nextAction.contains(Query) ||
              state.starts == 0 || state.remainingPolls <= 0 || poll != state.polls + 1)
            bad("invalid child query intent or exhausted poll budget")
          else state.copy(polls = poll, remainingPolls = state.remainingPolls - 1, nextPollAt = None)

        case ChildRunning(identity, next) if canObserve =>
          if (!correlated(identity) || next.isDefined != (state.remainingPolls > 0))
            bad("invalid child identity or polling schedule")
          else state.copy(phase = if (next.isDefined) Phase.Running else Phase.Blocked,
            childIdentity = Some(identity), result = None, nextAction = next.map(_ => Query), nextPollAt = next,
            reason = if (next.isDefined) None else Some(Exhausted))

        case ChildTerminalRecorded(identity, phase, bytes, reason) if canObserve =>
          if (!correlated(identity)) bad("terminal does not identify the frozen child run")
          else state.copy(phase = phase, childIdentity = Some(identity), result = Some(bytes), reason = reason,
            nextAction = None, nextPollAt = None)

        case Blocked(reason) if state.request.nonEmpty && state.starts > 0 =>
          state.copy(phase = Phase.Blocked, reason = Some(reason), nextAction = None, nextPollAt = None)

        case Reconciled(round, due) if state.phase == Phase.Blocked && state.policy.nonEmpty && state.nextAction.isEmpty =>
          if (round != state.reconciliations + 1 || round > state.policy.get.maxReconciliations)
            bad("invalid reconciliation round or exhausted lifetime budget")
          // Still Blocked until an authoritative child observation changes that fact.
          else state.copy(reconciliations = round, remainingPolls = state.policy.get.maxPolls,
            nextAction = Some(Query), nextPollAt = Some(due))

        case _ => bad("event is not valid at the recorded delivery frontier")
      }
    }
  }

  def recoveryProblem(state: DeliveryState, deliveryId: String, limits: ProtocolLimits): Option[String] =
    state.invalid.orElse(DeliveryValidation.state(state, limits)).orElse {
      state.request.flatMap(r => if (r.deliveryId != deliveryId) Some("recorded delivery id differs from this entity") else None)
    }
}
