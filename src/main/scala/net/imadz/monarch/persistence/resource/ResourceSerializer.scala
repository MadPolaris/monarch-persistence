package net.imadz.monarch.persistence.resource

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.SerializerWithStringManifest
import net.imadz.monarch.persistence.resource.ResourceProtocol.Wire
import net.imadz.monarch.persistence.resource.proto.{ResourceEventPO, ResourceStatePO, ResourceWirePO}

final class ResourceSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  override val identifier: Int = 77102140
  private val limits = ResourceValidation.Ceilings
  override def manifest(value: AnyRef): String = value match {
    case _: ResourceEvent => "RE1"
    case _: ResourceState => "RS1"
    case _ => throw new IllegalArgumentException("unsupported resource persistence value")
  }
  override def toBinary(value: AnyRef): Array[Byte] = {
    val proto: scalapb.GeneratedMessage = value match {
      case e: ResourceEvent =>
        ResourceValidation.requireValid(ResourceValidation.event(e)); ResourceProtoConverters.eventToProto(e)
      case s: ResourceState =>
        ResourceValidation.requireValid(ResourceValidation.state(s)); ResourceProtoConverters.stateToProto(s)
      case _ => throw new IllegalArgumentException("unsupported resource persistence value")
    }
    require(proto.serializedSize <= limits.maxEnvelopeBytes, "resource envelope exceeds limit")
    proto.toByteArray
  }
  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    require(bytes != null && bytes.length <= limits.maxEnvelopeBytes, "resource envelope exceeds limit")
    manifest match {
      case "RE1" => ResourceProtoConverters.eventFromProto(ResourceEventPO.parseFrom(bytes))
      case "RS1" => ResourceProtoConverters.stateFromProto(ResourceStatePO.parseFrom(bytes))
      case _ => throw new IllegalArgumentException("unsupported resource persistence manifest")
    }
  }
}

/** Only Wire, never the local Command hierarchy, has a remote serializer binding. */
final class ResourceWireSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  override val identifier: Int = 77102141
  private val resolver = ActorRefResolver(system.toTyped)
  private val limits = ResourceValidation.Ceilings
  override def manifest(value: AnyRef): String = value match {
    case _: Wire => "RW1"
    case _ => throw new IllegalArgumentException("unsupported resource wire value")
  }
  override def toBinary(value: AnyRef): Array[Byte] = value match {
    case wire: Wire =>
      val proto = ResourceProtoConverters.wireToProto(wire, resolver)
      require(proto.serializedSize <= limits.maxEnvelopeBytes, "resource wire envelope exceeds limit")
      proto.toByteArray
    case _ => throw new IllegalArgumentException("unsupported resource wire value")
  }
  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    require(bytes != null && bytes.length <= limits.maxEnvelopeBytes, "resource wire envelope exceeds limit")
    require(manifest == "RW1", "unsupported resource wire manifest")
    ResourceProtoConverters.wireFromProto(ResourceWirePO.parseFrom(bytes), resolver)
  }
}
