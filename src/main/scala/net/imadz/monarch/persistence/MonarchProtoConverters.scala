package net.imadz.monarch.persistence

import com.google.protobuf.ByteString
import net.imadz.monarch.persistence.PersistentMonarch._
import net.imadz.monarch.persistence.proto._

/** The Scala API is stable independently of protobuf wire identities. */
private[persistence] object MonarchProtoConverters {
  def eventToProto(event: Event): EventPO = {
    val payload = event match {
      case RunStarted(id, plan, bytes) => EventPO.Event.Started(RunStartedPO(id, plan, ByteString.copyFrom(bytes)))
      case StageDone(cursor, time, metadata, bytes) => EventPO.Event.Done(StageDonePO(cursor, time, metadata, ByteString.copyFrom(bytes)))
      case StageResolved(cursor, time, code, kind, detail, bytes) =>
        EventPO.Event.Resolved(StageResolvedPO(cursor, time, code, kind, detail, ByteString.copyFrom(bytes)))
      case RunCompleted(id) => EventPO.Event.Completed(RunCompletedPO(id))
      case RunFailedEvt(cursor, reason) => EventPO.Event.Failed(RunFailedPO(cursor, reason))
      case RunPlanExtended(plan) => EventPO.Event.PlanExtended(RunPlanExtendedPO(plan))
    }
    EventPO(schemaVersion = 1, event = payload)
  }

  def eventFromProto(proto: EventPO): Event = {
    require(proto.schemaVersion == 1, s"unsupported event schema version ${proto.schemaVersion}")
    proto.event match {
      case EventPO.Event.Started(p) => RunStarted(p.runId, p.stageCursors.toVector, p.initialSnapshot.toByteArray)
      case EventPO.Event.Done(p) => StageDone(p.cursor, p.timestamp, p.metadata, p.snapshot.toByteArray)
      case EventPO.Event.Resolved(p) => StageResolved(p.cursor, p.timestamp, p.code, p.errorCode, p.detail, p.snapshot.toByteArray)
      case EventPO.Event.Completed(p) => RunCompleted(p.runId)
      case EventPO.Event.Failed(p) => RunFailedEvt(p.cursor, p.reason)
      case EventPO.Event.PlanExtended(p) => RunPlanExtended(p.stageCursors.toVector)
      case EventPO.Event.Empty => throw new IllegalArgumentException("missing/unknown event variant")
    }
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
    DurableStatePO(schemaVersion = 1, run = Some(stateToProto(state.run)),
      plan = state.plan, invalid = state.invalid, stateVersion = state.version)

  def snapshotFromProto(proto: DurableStatePO): DurableState = {
    require(proto.schemaVersion == 1, s"unsupported snapshot schema version ${proto.schemaVersion}")
    DurableState(stateFromProto(proto.run.getOrElse(throw new IllegalArgumentException("missing snapshot run state"))),
      proto.plan.toVector, proto.invalid, proto.stateVersion)
  }
}
