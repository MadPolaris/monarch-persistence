package net.imadz.monarch.persistence.operation

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializerWithStringManifest
import net.imadz.monarch.persistence.operation.OperationProtocol.Wire
import net.imadz.monarch.persistence.operation.proto.{OperationEventPO, OperationStatePO, OperationWirePO}

/** IDs are distinct from both the root Monarch serializer and test fixtures. */
final class OperationSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  private val limits = OperationSettings.fromConfig(system.settings.config).limits
  override val identifier: Int = 77102130

  override def manifest(value: AnyRef): String = value match {
    case _: OperationEvent => "OE1"
    case _: OperationState => "OS1"
    case _ => throw new IllegalArgumentException("unsupported operation persistence value")
  }

  override def toBinary(value: AnyRef): Array[Byte] = {
    val proto: scalapb.GeneratedMessage = value match {
      case event: OperationEvent =>
        OperationValidation.requireValid(OperationValidation.event(event, limits))
        OperationProtoConverters.eventToProto(event)
      case state: OperationState =>
        OperationValidation.requireValid(OperationValidation.state(state, limits))
        OperationProtoConverters.stateToProto(state)
      case _ => throw new IllegalArgumentException("unsupported operation persistence value")
    }
    require(proto.serializedSize <= limits.maxEnvelopeBytes, "operation envelope exceeds its byte limit")
    proto.toByteArray
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    // Bound allocation before protobuf parses strings or nested byte arrays.
    require(bytes != null && bytes.length <= limits.maxEnvelopeBytes, "operation envelope exceeds its byte limit")
    manifest match {
      case "OE1" => OperationProtoConverters.eventFromProto(OperationEventPO.parseFrom(bytes), limits)
      case "OS1" => OperationProtoConverters.stateFromProto(OperationStatePO.parseFrom(bytes), limits)
      case _ => throw new IllegalArgumentException("unsupported operation persistence manifest")
    }
  }
}

/** Only OperationProtocol.Wire is bound, never the internal Command hierarchy. */
final class OperationWireSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  private val limits = OperationSettings.fromConfig(system.settings.config).limits
  private val resolver = ActorRefResolver(system.toTyped)
  override val identifier: Int = 77102131

  override def manifest(value: AnyRef): String = value match {
    case _: Wire => "OW1"
    case _ => throw new IllegalArgumentException("unsupported operation wire value")
  }

  override def toBinary(value: AnyRef): Array[Byte] = value match {
    case wire: Wire =>
      val proto = OperationProtoConverters.wireToProto(wire, resolver, limits)
      require(proto.serializedSize <= limits.maxEnvelopeBytes, "operation wire envelope exceeds its byte limit")
      proto.toByteArray
    case _ => throw new IllegalArgumentException("unsupported operation wire value")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    require(bytes != null && bytes.length <= limits.maxEnvelopeBytes, "operation wire envelope exceeds its byte limit")
    require(manifest == "OW1", "unsupported operation wire manifest")
    OperationProtoConverters.wireFromProto(OperationWirePO.parseFrom(bytes), resolver, limits)
  }
}
