package net.imadz.monarch.persistence.delivery

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializerWithStringManifest
import net.imadz.monarch.persistence.delivery.DeliveryProtocol.Wire
import net.imadz.monarch.persistence.delivery.proto.{DeliveryEventPO, DeliveryStatePO, DeliveryWirePO}

final class DeliverySerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  private val limits = DeliverySettings.fromConfig(system.settings.config).limits
  override val identifier: Int = 77102150

  override def manifest(value: AnyRef): String = value match {
    case _: DeliveryEvent => "DE1"
    case _: DeliveryState => "DS1"
    case _ => throw new IllegalArgumentException("unsupported delivery persistence value")
  }

  override def toBinary(value: AnyRef): Array[Byte] = {
    val proto: scalapb.GeneratedMessage = value match {
      case event: DeliveryEvent =>
        DeliveryValidation.requireValid(DeliveryValidation.event(event, limits))
        DeliveryProtoConverters.eventToProto(event)
      case state: DeliveryState =>
        DeliveryValidation.requireValid(DeliveryValidation.state(state, limits))
        DeliveryProtoConverters.stateToProto(state)
      case _ => throw new IllegalArgumentException("unsupported delivery persistence value")
    }
    require(proto.serializedSize <= limits.maxEnvelopeBytes, "delivery envelope exceeds its byte limit")
    proto.toByteArray
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    // Reject oversized envelopes before protobuf allocates nested values.
    require(bytes != null && bytes.length <= limits.maxEnvelopeBytes, "delivery envelope exceeds its byte limit")
    manifest match {
      case "DE1" => DeliveryProtoConverters.eventFromProto(DeliveryEventPO.parseFrom(bytes), limits)
      case "DS1" => DeliveryProtoConverters.stateFromProto(DeliveryStatePO.parseFrom(bytes), limits)
      case _ => throw new IllegalArgumentException("unsupported delivery persistence manifest")
    }
  }
}

/** Wire excludes the Command messages carrying local callback futures/results. */
final class DeliveryWireSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  private val limits = DeliverySettings.fromConfig(system.settings.config).limits
  private val resolver = ActorRefResolver(system.toTyped)
  override val identifier: Int = 77102151

  override def manifest(value: AnyRef): String = value match {
    case _: Wire => "DW1"
    case _ => throw new IllegalArgumentException("unsupported delivery wire value")
  }

  override def toBinary(value: AnyRef): Array[Byte] = value match {
    case wire: Wire => DeliveryProtoConverters.wireToProto(wire, resolver, limits).toByteArray
    case _ => throw new IllegalArgumentException("unsupported delivery wire value")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    require(bytes != null && bytes.length <= limits.maxEnvelopeBytes, "delivery wire envelope exceeds its byte limit")
    require(manifest == "DW1", "unsupported delivery wire manifest")
    DeliveryProtoConverters.wireFromProto(DeliveryWirePO.parseFrom(bytes), resolver, limits)
  }
}
