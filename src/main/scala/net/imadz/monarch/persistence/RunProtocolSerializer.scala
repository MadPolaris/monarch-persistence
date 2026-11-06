package net.imadz.monarch.persistence

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializerWithStringManifest
import akka.util.ByteString
import com.google.protobuf.{ByteString => ProtoBytes}
import net.imadz.monarch.persistence.RunProtocol._
import net.imadz.monarch.persistence.proto._

/** Only concrete public envelopes are bound to this serializer. */
final class RunProtocolSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  override val identifier: Int = 77102123
  private val resolver = ActorRefResolver(system.toTyped)
  private val limits = ProtocolLimits()

  override def manifest(value: AnyRef): String = value match {
    case _: Wire => "RW1"
    case other => throw new IllegalArgumentException(s"unsupported run wire value ${other.getClass.getName}")
  }

  private def check(error: Option[String]): Unit = error.foreach(reason => throw new IllegalArgumentException(reason))
  private def text(name: String, value: String, empty: Boolean = false): Unit = check(limits.text(name, value, empty))
  private def identity(value: Identity): Unit = {
    text("runId", value.runId); text("definitionId", value.definitionId)
    text("definitionVersion", value.definitionVersion); text("codecVersion", value.codecVersion)
    text("initialDigest", value.initialDigest); text("planDigest", value.planDigest)
  }
  private def validate(value: Wire): Unit = value match {
    case s: StartOrAttach =>
      text("runId", s.runId); text("definitionId", s.definitionId)
      text("definitionVersion", s.definitionVersion); text("codecVersion", s.codecVersion)
      check(limits.bytes("initialState", s.initialState, limits.maxStateBytes))
    case GetRun(_) => ()
    case Rejected(id, reason) => text("runId", id, empty = true); text("reason", reason)
    case Unavailable(id, reason) => id.foreach(text("runId", _, empty = true)); text("reason", reason)
    case Current(view) =>
      view.runId.foreach(text("runId", _))
      check(limits.plan(view.completedCursors))
      view.lastState.foreach(bytes => check(limits.bytes("lastState", bytes, limits.maxStateBytes)))
      view.identity.foreach(identity)
      view.awaiting.foreach { a => text("cursor", a.cursor); text("operationKey", a.operationKey); text("requestDigest", a.requestDigest) }
      view.reason.foreach(text("reason", _))
  }

  private def status(name: String): Status = name match {
    case "NotStarted" => Status.NotStarted
    case "Running" => Status.Running
    case "AwaitingOperation" => Status.AwaitingOperation
    case "Completed" => Status.Completed
    case "Failed" => Status.Failed
    case "RecoveryBlocked" => Status.RecoveryBlocked
    case _ => throw new IllegalArgumentException(s"unknown run status $name")
  }

  override def toBinary(value: AnyRef): Array[Byte] = {
    val wire = value match { case w: Wire => w; case _ => throw new IllegalArgumentException("not a run wire envelope") }
    validate(wire)
    val payload = wire match {
      case s: StartOrAttach => RunWirePO.Value.Start(StartOrAttachPO(s.runId, s.definitionId, s.definitionVersion,
        s.codecVersion, ProtoBytes.copyFrom(s.initialState.toArray), resolver.toSerializationFormat(s.replyTo)))
      case GetRun(replyTo) => RunWirePO.Value.Get(GetRunPO(resolver.toSerializationFormat(replyTo)))
      case Current(view) => RunWirePO.Value.Current(RunViewPO(view.runId, view.status.toString,
        view.completedCursors, view.lastState.map(bytes => ProtoBytes.copyFrom(bytes.toArray)),
        view.identity.map(MonarchProtoConverters.identityToProto), view.awaiting.map(MonarchProtoConverters.awaitingToProto), view.reason))
      case Rejected(id, reason) => RunWirePO.Value.Rejected(RunRejectedPO(id, reason))
      case Unavailable(id, reason) => RunWirePO.Value.Unavailable(RunUnavailablePO(id, reason))
    }
    val bytes = RunWirePO(schemaVersion = 1, value = payload).toByteArray
    require(bytes.length <= limits.maxEnvelopeBytes, "run envelope exceeds size limit")
    bytes
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    require(manifest == "RW1", s"unknown run wire manifest $manifest")
    require(bytes.length <= limits.maxEnvelopeBytes, "run envelope exceeds size limit")
    val proto = RunWirePO.parseFrom(bytes)
    require(proto.schemaVersion == 1, s"unknown run wire version ${proto.schemaVersion}")
    val value: Wire = proto.value match {
      case RunWirePO.Value.Start(p) =>
        text("replyTo", p.replyTo)
        StartOrAttach(p.runId, p.definitionId, p.definitionVersion, p.codecVersion,
          ByteString.fromArray(p.initialState.toByteArray), resolver.resolveActorRef[Reply](p.replyTo))
      case RunWirePO.Value.Get(p) => text("replyTo", p.replyTo); GetRun(resolver.resolveActorRef[Reply](p.replyTo))
      case RunWirePO.Value.Current(p) => Current(View(p.runId, status(p.status), p.completedCursors.toVector,
        p.lastState.map(bytes => ByteString.fromArray(bytes.toByteArray)), p.identity.map(MonarchProtoConverters.identityFromProto),
        p.awaiting.map(MonarchProtoConverters.awaitingFromProto), p.reason))
      case RunWirePO.Value.Rejected(p) => Rejected(p.runId, p.reason)
      case RunWirePO.Value.Unavailable(p) => Unavailable(p.runId, p.reason)
      case RunWirePO.Value.Empty => throw new IllegalArgumentException("missing/unknown run wire variant")
    }
    validate(value)
    value
  }
}
