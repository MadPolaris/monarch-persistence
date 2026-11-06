package net.imadz.monarch.persistence

import akka.serialization.SerializerWithStringManifest
import net.imadz.monarch.persistence.PersistentMonarch._
import net.imadz.monarch.persistence.proto.{DurableStatePO, EventPO, RunStatePO}

/** New writes use protobuf. Akka retains the serializer id and manifest with each
  * record, so historical Jackson records are still decoded by their old serializer.
  */
final class MonarchSerializer extends SerializerWithStringManifest {
  override val identifier: Int = 77102121

  override def manifest(value: AnyRef): String = value match {
    case _: VersionedRunStarted | _: RunAwaiting => "E2"
    case _: Event => "E1"
    case _: DurableState => "S2"
    case _: RunState => "R1"
    case other => throw new IllegalArgumentException(s"unsupported Monarch value ${other.getClass.getName}")
  }

  override def toBinary(value: AnyRef): Array[Byte] = value match {
    case event: Event => MonarchProtoConverters.eventToProto(event).toByteArray
    case state: DurableState => MonarchProtoConverters.snapshotToProto(state).toByteArray
    case state: RunState => MonarchProtoConverters.stateToProto(state).toByteArray
    case other => throw new IllegalArgumentException(s"unsupported Monarch value ${other.getClass.getName}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case "E1" | "E2" =>
      val proto = EventPO.parseFrom(bytes)
      require(proto.schemaVersion == (if (manifest == "E1") 1 else 2), "event manifest/schema mismatch")
      MonarchProtoConverters.eventFromProto(proto)
    case "S1" | "S2" =>
      val proto = DurableStatePO.parseFrom(bytes)
      require(proto.schemaVersion == (if (manifest == "S1") 1 else 2), "snapshot manifest/schema mismatch")
      MonarchProtoConverters.snapshotFromProto(proto)
    case "R1" => MonarchProtoConverters.stateFromProto(RunStatePO.parseFrom(bytes))
    case other => throw new IllegalArgumentException(s"unsupported Monarch manifest $other")
  }
}
