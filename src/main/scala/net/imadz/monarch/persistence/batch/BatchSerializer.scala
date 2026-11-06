package net.imadz.monarch.persistence.batch

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializerWithStringManifest
import net.imadz.monarch.persistence.batch.BatchProtocol.Wire
import net.imadz.monarch.persistence.batch.proto.{BatchEventPO, BatchStatePO, BatchWirePO}

final class BatchSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  override val identifier: Int = 77102160
  override def manifest(value: AnyRef): String = value match {
    case _: BatchEvent => "BE1"
    case _: BatchState => "BS1"
    case _ => throw new IllegalArgumentException("unsupported batch persistence value")
  }
  override def toBinary(value: AnyRef): Array[Byte] = {
    val proto: scalapb.GeneratedMessage = value match {
      case event: BatchEvent => BatchCodec.eventToProto(event)
      case state: BatchState => BatchCodec.stateToProto(state)
      case _ => throw new IllegalArgumentException("unsupported batch persistence value")
    }
    require(proto.serializedSize <= BatchCodec.MaxEnvelopeBytes, "batch persistence envelope exceeds its byte limit")
    proto.toByteArray
  }
  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    // Check before protobuf allocates nested strings, collections or byte arrays.
    require(bytes != null && bytes.length <= BatchCodec.MaxEnvelopeBytes, "batch persistence envelope exceeds its byte limit")
    manifest match {
      case "BE1" => BatchCodec.eventFromProto(BatchEventPO.parseFrom(bytes))
      case "BS1" => BatchCodec.stateFromProto(BatchStatePO.parseFrom(bytes))
      case _ => throw new IllegalArgumentException("unsupported batch persistence manifest")
    }
  }
}

/** Deliberately binds only public Wire DTOs, not Command or coordinator callbacks. */
final class BatchWireSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  private val resolver = ActorRefResolver(system.toTyped)
  override val identifier: Int = 77102161
  override def manifest(value: AnyRef): String = value match {
    case _: Wire => "BW1"
    case _ => throw new IllegalArgumentException("unsupported batch wire value")
  }
  override def toBinary(value: AnyRef): Array[Byte] = value match {
    case wire: Wire => BatchCodec.wireToProto(wire, resolver).toByteArray
    case _ => throw new IllegalArgumentException("unsupported batch wire value")
  }
  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    require(bytes != null && bytes.length <= BatchCodec.MaxWireBytes, "batch wire envelope exceeds its byte limit")
    require(manifest == "BW1", "unsupported batch wire manifest")
    BatchCodec.wireFromProto(BatchWirePO.parseFrom(bytes), resolver)
  }
}
