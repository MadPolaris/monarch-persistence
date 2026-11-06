package net.imadz.monarch.persistence.delivery

import akka.util.ByteString
import net.imadz.monarch.persistence.{ProtocolDigest, ProtocolLimits, RunProtocol, RunSubmission}
import net.imadz.monarch.persistence.delivery.DeliveryProtocol._

import java.nio.charset.StandardCharsets.UTF_8

/** Shared fail-closed shape, identity and aggregate-envelope checks. */
private[persistence] object DeliveryValidation {
  private def optional[A](name: String, value: Option[A])(check: A => Option[String]): Option[String] =
    if (value == null) Some(s"$name must not be null") else value.flatMap(check)

  private def digest(name: String, value: String, limits: ProtocolLimits): Option[String] =
    limits.text(name, value).orElse(if (value.matches("[0-9a-f]{64}")) None else Some(s"$name must be a lowercase SHA-256"))

  private def due(value: Option[Long]): Option[String] =
    optional("next poll time", value)(n => if (n < 0L) Some("negative next poll time") else None)

  private def size(value: String): Long = value.getBytes(UTF_8).length.toLong
  private def identitySize(id: RunProtocol.Identity): Long =
    Vector(id.runId, id.definitionId, id.definitionVersion, id.codecVersion, id.initialDigest, id.planDigest).map(size).sum
  private def requestSize(r: Request): Long = Vector(r.deliveryId, r.sourceRunId, r.target, r.child.runId,
    r.child.definitionId, r.child.definitionVersion, r.child.codecVersion).map(size).sum +
    r.payload.length.toLong + r.child.initialState.length
  private def envelope(bytes: Long, limits: ProtocolLimits): Option[String] =
    if (bytes > limits.maxEnvelopeBytes) Some("delivery aggregate envelope exceeds its byte limit") else None

  def request(value: Request, limits: ProtocolLimits): Option[String] =
    if (value == null) Some("delivery request must not be null")
    else limits.text("delivery id", value.deliveryId).orElse(limits.text("source run id", value.sourceRunId))
      .orElse(limits.text("delivery target", value.target))
      .orElse(if (value.child == null) Some("child submission is required") else value.child.validate(limits))
      .orElse(limits.bytes("delivery payload", value.payload, limits.maxRequestBytes))
      .orElse(envelope(requestSize(value) + 2L * limits.maxStringBytes + 512L, limits))

  def identity(value: RunProtocol.Identity, limits: ProtocolLimits): Option[String] =
    if (value == null) Some("child run identity is required")
    else limits.text("child run id", value.runId).orElse(limits.text("child definition id", value.definitionId))
      .orElse(limits.text("child definition version", value.definitionVersion))
      .orElse(limits.text("child codec version", value.codecVersion))
      .orElse(digest("child initial digest", value.initialDigest, limits))
      .orElse(digest("child plan digest", value.planDigest, limits))

  def correlate(child: RunSubmission, id: RunProtocol.Identity, previous: Option[RunProtocol.Identity]): Option[String] =
    if (id == null) Some("child run identity is missing")
    else if (id.runId != child.runId) Some("child run id mismatch")
    else if (id.definitionId != child.definitionId || id.definitionVersion != child.definitionVersion)
      Some("child definition identity/version mismatch")
    else if (id.codecVersion != child.codecVersion) Some("child codec version mismatch")
    else if (id.initialDigest != ProtocolDigest.sha256(child.initialState)) Some("child initial state digest mismatch")
    else if (previous.exists(_.planDigest != id.planDigest)) Some("child frozen plan digest changed")
    else None

  def runView(value: RunProtocol.View, child: RunSubmission, previous: Option[RunProtocol.Identity],
      limits: ProtocolLimits): Option[String] = {
    if (value == null) Some("child returned no committed view")
    else optional("child run id", value.runId)(limits.text("child run id", _))
      .orElse(optional("child identity", value.identity)(identity(_, limits)))
      .orElse(optional("child last state", value.lastState)(limits.bytes("child last state", _, limits.maxStateBytes)))
      .orElse(optional("child reason", value.reason)(limits.text("child reason", _)))
      .orElse(if (value.completedCursors == null) Some("child cursor vector is missing") else limits.plan(value.completedCursors))
      .orElse(optional("child awaiting reference", value.awaiting) { ref =>
        if (ref == null) Some("child awaiting reference is null")
        else limits.text("child awaiting cursor", ref.cursor).orElse(limits.text("child operation key", ref.operationKey))
          .orElse(limits.text("child operation request digest", ref.requestDigest))
      }).orElse {
        val bytes = value.lastState.fold(0L)(_.length.toLong) + value.identity.fold(0L)(identitySize) +
          value.completedCursors.map(size).sum + value.runId.fold(0L)(size) + value.reason.fold(0L)(size) +
          value.awaiting.fold(0L)(a => size(a.cursor) + size(a.operationKey) + size(a.requestDigest)) + 1024L
        envelope(bytes, limits)
      }.orElse {
        import RunProtocol.Status
        value.status match {
          case Status.NotStarted =>
            if (value.runId.nonEmpty || value.identity.nonEmpty || value.lastState.nonEmpty ||
                value.awaiting.nonEmpty || value.completedCursors.nonEmpty)
              Some("NotStarted child view contains committed run history") else None
          case Status.Running | Status.AwaitingOperation | Status.Completed | Status.Failed =>
            if (!value.runId.contains(child.runId)) Some("child view run id mismatch")
            else if (value.identity.isEmpty) Some("committed child identity is missing")
            else correlate(child, value.identity.get, previous).orElse {
              if (value.lastState.isEmpty) Some("child committed state/result is unavailable")
              else if (value.status == Status.AwaitingOperation && value.awaiting.isEmpty)
                Some("awaiting child has no committed operation reference")
              else if (value.status == Status.Completed || value.status == Status.Failed)
                limits.bytes("child terminal result", value.lastState.get, limits.maxResultBytes)
              else None
            }
          case Status.RecoveryBlocked => None // Caller blocks; it never adopts this identity or result.
          case _ => Some("unknown child run status")
        }
      }
  }

  def view(value: View, limits: ProtocolLimits): Option[String] =
    if (value == null) Some("delivery view is required")
    else limits.text("delivery id", value.deliveryId)
      .orElse(optional("request digest", value.requestDigest)(digest("request digest", _, limits)))
      .orElse(optional("child run id", value.childRunId)(limits.text("child run id", _)))
      .orElse(optional("delivery result", value.result)(limits.bytes("delivery result", _, limits.maxResultBytes)))
      .orElse(optional("delivery reason", value.reason)(limits.text("delivery reason", _)))
      .orElse {
        if (!Phase.values.contains(value.phase)) Some("unknown delivery phase")
        else if (value.reconciling && value.phase != Phase.Blocked) Some("query reconciliation requires a blocked delivery")
        else if (value.phase == Phase.Empty && (value.requestDigest.nonEmpty || value.childRunId.nonEmpty || value.reason.nonEmpty))
          Some("empty delivery view contains accepted history")
        else if (value.phase != Phase.Empty && value.phase != Phase.RecoveryBlocked &&
            (value.requestDigest.isEmpty || value.childRunId.isEmpty)) Some("delivery view lacks its immutable identity")
        else if (value.result.isDefined != Phase.terminal.contains(value.phase)) Some("delivery phase and terminal result disagree")
        else if (Set(Phase.Blocked, Phase.RecoveryBlocked, Phase.Failed).contains(value.phase) && value.reason.isEmpty)
          Some("blocked/failed delivery view requires a reason")
        else if (Set(Phase.Accepted, Phase.Starting, Phase.Running, Phase.Completed).contains(value.phase) && value.reason.nonEmpty)
          Some("active/successful delivery view contains a failure reason")
        else envelope(size(value.deliveryId) + value.requestDigest.fold(0L)(size) + value.childRunId.fold(0L)(size) +
          value.result.fold(0L)(_.length.toLong) + value.reason.fold(0L)(size) + 512L, limits)
      }

  def event(value: DeliveryEvent, limits: ProtocolLimits): Option[String] = {
    import DeliveryEvent._
    value match {
      case Accepted(r, d, p) => request(r, limits).orElse(digest("request digest", d, limits))
        .orElse(if (p == null) Some("delivery policy is missing") else request(r, p.limits))
        .orElse(if (d != r.fingerprint) Some("accepted request fingerprint mismatch") else None)
        .orElse(envelope(requestSize(r) + size(d) + 1024L, limits))
      case ChildStartIntent(n) => if (n <= 0) Some("invalid child start count") else None
      case ChildQueryIntent(n) => if (n <= 0) Some("invalid child poll count") else None
      case ChildRunning(id, next) => identity(id, limits).orElse(due(next))
        .orElse(envelope(identitySize(id) + 512L, limits))
      case ChildTerminalRecorded(id, phase, bytes, reason) => identity(id, limits)
        .orElse(limits.bytes("child terminal result", bytes, limits.maxResultBytes))
        .orElse(limits.bytes("child terminal state", bytes, limits.maxStateBytes))
        .orElse(optional("child terminal reason", reason)(limits.text("child terminal reason", _)))
        .orElse {
          if (!Phase.terminal.contains(phase)) Some("child terminal phase is not Completed or Failed")
          else if ((phase == Phase.Failed) != reason.isDefined) Some("child terminal reason and phase disagree")
          else envelope(identitySize(id) + bytes.length.toLong + reason.fold(0L)(size) + 512L, limits)
        }
      case Blocked(reason) => limits.text("blocked delivery reason", reason)
      case Reconciled(round, next) => if (round <= 0 || next < 0L) Some("invalid delivery reconciliation grant") else None
      case null => Some("delivery event is required")
    }
  }

  private def stateFields(value: DeliveryState, limits: ProtocolLimits): Option[String] =
    optional("delivery request", value.request)(request(_, limits))
      .orElse(optional("request digest", value.requestDigest)(digest("request digest", _, limits)))
      .orElse(optional("delivery policy", value.policy)(p => if (p == null) Some("delivery policy is null") else None))
      .orElse(optional("child identity", value.childIdentity)(identity(_, limits)))
      .orElse(optional("delivery result", value.result)(b => limits.bytes("delivery result", b, limits.maxResultBytes)
        .orElse(limits.bytes("child terminal state", b, limits.maxStateBytes))))
      .orElse(optional("delivery reason", value.reason)(limits.text("delivery reason", _)))
      .orElse(optional("invalid history", value.invalid)(limits.text("invalid history", _)))
      .orElse(optional("next delivery action", value.nextAction)(a =>
        if (a == DeliveryState.Ensure || a == DeliveryState.Query) None else Some("unknown next delivery action")))
      .orElse(due(value.nextPollAt))
      .orElse(envelope(value.request.fold(0L)(requestSize) + value.childIdentity.fold(0L)(identitySize) +
        value.requestDigest.fold(0L)(size) + value.result.fold(0L)(_.length.toLong) +
        value.reason.fold(0L)(size) + value.invalid.fold(0L)(size) + 1024L, limits))

  def state(value: DeliveryState, limits: ProtocolLimits): Option[String] = {
    if (value == null) Some("delivery state is required")
    else stateFields(value, limits).orElse(value.policy.flatMap(p => stateFields(value, p.limits))).orElse {
      if (value.version != 1) Some("unsupported delivery state version")
      else if (!Phase.values.contains(value.phase) || value.phase == Phase.RecoveryBlocked) Some("unknown persisted delivery phase")
      else if (Vector(value.starts, value.polls, value.remainingPolls, value.reconciliations).exists(_ < 0))
        Some("negative delivery counter")
      else value.request match {
        case None =>
          if (value.phase != Phase.Empty || value.requestDigest.nonEmpty || value.policy.nonEmpty ||
              value.childIdentity.nonEmpty || value.result.nonEmpty || value.reason.nonEmpty || value.nextAction.nonEmpty ||
              value.nextPollAt.nonEmpty || value.starts != 0 || value.polls != 0 || value.remainingPolls != 0 || value.reconciliations != 0)
            Some("empty delivery contains accepted history") else None
        case Some(r) => value.policy match {
          case None => Some("accepted delivery has no frozen policy")
          case Some(p) =>
            if (!value.requestDigest.contains(r.fingerprint)) Some("recorded delivery request digest mismatch")
            else if (value.reconciliations > p.maxReconciliations || value.starts > p.maxStarts ||
                value.remainingPolls > p.maxPolls || value.polls.toLong + value.remainingPolls >
                  p.maxPolls.toLong * (value.reconciliations.toLong + 1L)) Some("delivery counters exceed frozen policy")
            else if (value.phase == Phase.Empty) Some("accepted request has empty phase")
            else if (value.phase == Phase.Accepted && (value.starts != 0 || value.polls != 0 || value.reconciliations != 0 ||
                value.childIdentity.nonEmpty || value.remainingPolls != p.maxPolls || !value.nextAction.contains(DeliveryState.Ensure)))
              Some("invalid accepted delivery frontier")
            else if (value.phase != Phase.Accepted && value.starts == 0) Some("child has no committed start intent")
            else if (value.phase == Phase.Starting && !value.nextAction.contains(DeliveryState.Ensure))
              Some("starting child has no same-child ensure intent")
            else if (value.phase == Phase.Running && (value.childIdentity.isEmpty || !value.nextAction.contains(DeliveryState.Query)))
              Some("running child lacks validated identity or query intent")
            else if (value.phase == Phase.Running && value.nextPollAt.isEmpty && value.polls == 0)
              Some("running child lacks a recorded query or schedule")
            else if (value.phase == Phase.Blocked && value.nextAction.nonEmpty &&
                (!value.nextAction.contains(DeliveryState.Query) || value.reconciliations == 0))
              Some("blocked child lacks an operator reconciliation grant")
            else if (value.nextPollAt.nonEmpty && (!value.nextAction.contains(DeliveryState.Query) || value.remainingPolls == 0))
              Some("scheduled child query has no intent or budget")
            else if (value.terminal && (value.childIdentity.isEmpty || value.nextAction.nonEmpty || value.nextPollAt.nonEmpty))
              Some("terminal child lacks identity or still has scheduled work")
            else value.childIdentity.flatMap(correlate(r.child, _, None)).orElse {
              view(View(r.deliveryId, value.requestDigest, Some(r.child.runId), value.phase, value.result, value.reason), limits)
            }
        }
      }
    }
  }

  def requireValid(problem: Option[String]): Unit = problem.foreach(p => throw new IllegalArgumentException(p))
}
