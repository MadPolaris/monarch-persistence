package net.imadz.monarch.persistence

import com.google.protobuf.ByteString
import net.imadz.monarch.persistence.PersistentMonarch._
import net.imadz.monarch.persistence.proto._

/** E1/S1 remain readable. Extended writes use E2/S2 so old readers fail closed. */
private[persistence] object MonarchProtoConverters {
  def identityToProto(value: RunProtocol.Identity): RunIdentityPO =
    RunIdentityPO(value.runId, value.definitionId, value.definitionVersion, value.codecVersion,
      value.initialDigest, value.planDigest)
  def identityFromProto(value: RunIdentityPO): RunProtocol.Identity =
    RunProtocol.Identity(value.runId, value.definitionId, value.definitionVersion, value.codecVersion,
      value.initialDigest, value.planDigest)
  def awaitingToProto(value: RunProtocol.AwaitingRef): AwaitingRefPO =
    AwaitingRefPO(value.cursor, value.operationKey, value.requestDigest)
  def awaitingFromProto(value: AwaitingRefPO): RunProtocol.AwaitingRef =
    RunProtocol.AwaitingRef(value.cursor, value.operationKey, value.requestDigest)

  def eventToProto(event: Event): EventPO = {
    val payload = event match {
      case RunStarted(id, plan, bytes) => EventPO.Event.Started(RunStartedPO(id, plan, ByteString.copyFrom(bytes)))
      case StageDone(cursor, time, metadata, bytes) => EventPO.Event.Done(StageDonePO(cursor, time, metadata, ByteString.copyFrom(bytes)))
      case StageResolved(cursor, time, code, kind, detail, bytes) =>
        EventPO.Event.Resolved(StageResolvedPO(cursor, time, code, kind, detail, ByteString.copyFrom(bytes)))
      case RunCompleted(id) => EventPO.Event.Completed(RunCompletedPO(id))
      case RunFailedEvt(cursor, reason) => EventPO.Event.Failed(RunFailedPO(cursor, reason))
      case RunPlanExtended(plan) => EventPO.Event.PlanExtended(RunPlanExtendedPO(plan))
      case VersionedRunStarted(identity, plan, bytes) => EventPO.Event.VersionedStarted(
        VersionedRunStartedPO(Some(identityToProto(identity)), plan, ByteString.copyFrom(bytes.toArray)))
      case RunAwaiting(reference) => EventPO.Event.Awaiting(awaitingToProto(reference))
    }
    val version = event match { case _: VersionedRunStarted | _: RunAwaiting => 2; case _ => 1 }
    EventPO(schemaVersion = version, event = payload)
  }

  def eventFromProto(proto: EventPO): Event = {
    require(proto.schemaVersion == 1 || proto.schemaVersion == 2, s"unsupported event schema version ${proto.schemaVersion}")
    val event = proto.event match {
      case EventPO.Event.Started(p) => RunStarted(p.runId, p.stageCursors.toVector, p.initialSnapshot.toByteArray)
      case EventPO.Event.Done(p) => StageDone(p.cursor, p.timestamp, p.metadata, p.snapshot.toByteArray)
      case EventPO.Event.Resolved(p) => StageResolved(p.cursor, p.timestamp, p.code, p.errorCode, p.detail, p.snapshot.toByteArray)
      case EventPO.Event.Completed(p) => RunCompleted(p.runId)
      case EventPO.Event.Failed(p) => RunFailedEvt(p.cursor, p.reason)
      case EventPO.Event.PlanExtended(p) => RunPlanExtended(p.stageCursors.toVector)
      case EventPO.Event.VersionedStarted(p) =>
        require(proto.schemaVersion == 2, "versioned start requires event schema 2")
        VersionedRunStarted(identityFromProto(p.identity.getOrElse(throw new IllegalArgumentException("missing run identity"))),
          p.stageCursors.toVector, akka.util.ByteString.fromArray(p.initialSnapshot.toByteArray))
      case EventPO.Event.Awaiting(p) =>
        require(proto.schemaVersion == 2, "operation reference requires event schema 2")
        RunAwaiting(awaitingFromProto(p))
      case EventPO.Event.Empty => throw new IllegalArgumentException("missing/unknown event variant")
    }
    event
  }

  def stateToProto(state: RunState): RunStatePO = {
    val payload = state match {
      case Idle => RunStatePO.State.Idle(IdlePO())
      case Executing(id, cursors, bytes) => RunStatePO.State.Executing(ExecutingPO(id, cursors, ByteString.copyFrom(bytes)))
      case Done(id) => RunStatePO.State.Done(DonePO(id))
      case Failed(id, cursor, reason) => RunStatePO.State.Failed(FailedPO(id, cursor, reason))
    }
    RunStatePO(state = payload)
  }

  def stateFromProto(proto: RunStatePO): RunState = proto.state match {
    case RunStatePO.State.Idle(_) => Idle
    case RunStatePO.State.Executing(p) => Executing(p.runId, p.completedCursors.toVector, p.lastSnapshot.toByteArray)
    case RunStatePO.State.Done(p) => Done(p.runId)
    case RunStatePO.State.Failed(p) => Failed(p.runId, p.cursor, p.reason)
    case RunStatePO.State.Empty => throw new IllegalArgumentException("missing/unknown run state variant")
  }

  def snapshotToProto(state: DurableState): DurableStatePO =
    DurableStatePO(schemaVersion = 2, run = Some(stateToProto(state.run)),
      plan = state.plan, invalid = state.invalid, stateVersion = state.version,
      identity = state.identity.map(identityToProto),
      retainedSnapshot = state.retainedSnapshot.map(bytes => ByteString.copyFrom(bytes.toArray)),
      completedCursors = state.completedCursors, awaiting = state.awaiting.map(awaitingToProto))

  def snapshotFromProto(proto: DurableStatePO): DurableState = {
    require(proto.schemaVersion == 1 || proto.schemaVersion == 2, s"unsupported snapshot schema version ${proto.schemaVersion}")
    if (proto.schemaVersion == 1) require(proto.identity.isEmpty && proto.retainedSnapshot.isEmpty &&
      proto.completedCursors.isEmpty && proto.awaiting.isEmpty, "schema 1 cannot contain extended snapshot fields")
    val run = stateFromProto(proto.run.getOrElse(throw new IllegalArgumentException("missing snapshot run state")))
    val retained = proto.retainedSnapshot.map(bytes => akka.util.ByteString.fromArray(bytes.toByteArray)).orElse {
      if (proto.schemaVersion == 1) run match {
        case e: Executing => Some(akka.util.ByteString.fromArray(e.lastSnapshot))
        case _ => None
      } else None
    }
    val completed = if (proto.schemaVersion == 1) run match {
      case e: Executing => e.completedCursors
      case _ => Vector.empty[String]
    } else proto.completedCursors.toVector
    DurableState(run, proto.plan.toVector, proto.invalid, proto.stateVersion,
      proto.identity.map(identityFromProto), retained, completed, proto.awaiting.map(awaitingFromProto))
  }
}
