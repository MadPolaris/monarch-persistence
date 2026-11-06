package net.imadz.monarch.persistence

import akka.actor.typed.ActorRef
import akka.util.ByteString

/** Concrete remote protocol. Internal checkpoint promises are deliberately not Wire values. */
object RunProtocol {
  sealed trait Wire
  sealed trait Request extends PersistentMonarch.PublicCommand with Wire
  final case class StartOrAttach(runId: String, definitionId: String, definitionVersion: String,
      codecVersion: String, initialState: ByteString, replyTo: ActorRef[Reply]) extends Request
  final case class GetRun(replyTo: ActorRef[Reply]) extends Request

  sealed trait Reply extends Wire
  final case class Current(view: View) extends Reply
  final case class Rejected(runId: String, reason: String) extends Reply
  final case class Unavailable(runId: Option[String], reason: String) extends Reply

  final case class Identity(runId: String, definitionId: String, definitionVersion: String,
      codecVersion: String, initialDigest: String, planDigest: String)
  final case class AwaitingRef(cursor: String, operationKey: String, requestDigest: String)

  sealed trait Status
  object Status {
    case object NotStarted extends Status
    case object Running extends Status
    case object AwaitingOperation extends Status
    case object Completed extends Status
    case object Failed extends Status
    case object RecoveryBlocked extends Status
  }

  /** lastState is the last committed checkpoint, not a failed operation's result.
    * A legacy terminal snapshot can have no lastState; absence never triggers re-execution.
    */
  final case class View(runId: Option[String], status: Status, completedCursors: Vector[String],
      lastState: Option[ByteString], identity: Option[Identity], awaiting: Option[AwaitingRef],
      reason: Option[String])

  private[persistence] def viewProblem(view: View, limits: ProtocolLimits): Option[String] =
    view.runId.flatMap(limits.text("runId", _)).orElse(limits.plan(view.completedCursors))
      .orElse(view.lastState.flatMap(limits.bytes("lastState", _, limits.maxStateBytes)))
      .orElse(view.identity.flatMap { id =>
        Vector(id.runId, id.definitionId, id.definitionVersion, id.codecVersion, id.initialDigest, id.planDigest)
          .iterator.flatMap(limits.text("run identity", _)).take(1).toVector.headOption
      }).orElse(view.awaiting.flatMap { a =>
        limits.text("cursor", a.cursor).orElse(limits.text("operationKey", a.operationKey))
          .orElse(limits.text("requestDigest", a.requestDigest))
      }).orElse(view.reason.flatMap(limits.text("reason", _)))
}

/** The host pins semantic and codec versions; the frozen ordered plan is also checked on recovery. */
final case class RunDefinition[Stage](
    id: String,
    version: String,
    codecVersion: String,
    stages: Vector[Stage],
    stepId: Stage => String = (stage: Stage) => String.valueOf(stage)
) {
  val stepIds: Vector[String] = stages.map(stepId)
  val cursors: Vector[String] = stepIds.zipWithIndex.map { case (name, index) => s"$name#$index" }
  val planDigest: String = ProtocolDigest.fields(cursors.map(ByteString(_)))

  private[persistence] def validate(limits: ProtocolLimits): Option[String] =
    limits.text("definitionId", id).orElse(limits.text("definitionVersion", version))
      .orElse(limits.text("codecVersion", codecVersion)).orElse(limits.plan(cursors))
      .orElse(stepIds.iterator.flatMap(limits.text("stepId", _)).take(1).toVector.headOption)
      .orElse(if (stepIds.distinct.size != stepIds.size) Some("versioned plans require distinct stable stepIds") else None)

  private[persistence] def identity(runId: String, bytes: ByteString): RunProtocol.Identity =
    RunProtocol.Identity(runId, id, version, codecVersion, ProtocolDigest.sha256(bytes), planDigest)
}
