package net.imadz.monarch.persistence

import akka.actor.typed.{ActorRefResolver, ActorSystem}
import akka.serialization.SerializerWithStringManifest
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.serialization.{SerializationExtension, Serializers}
import PersistentMonarch._

/** A representative host codec for a generic remote command. Persistence's
  * StateCodec alone cannot serialize StartRun's S and reply ActorRef envelope.
  */
final case class RemoteFixtureState(offset: Long, groups: Vector[String])

class HostCommandSerializer(system: akka.actor.ExtendedActorSystem) extends SerializerWithStringManifest {
  import akka.actor.typed.scaladsl.adapter._
  private val refs = ActorRefResolver(system.toTyped)
  override val identifier: Int = 77102122
  override def manifest(value: AnyRef): String = value match {
    case _: StartRun[_] => "start"
    case _: Accepted => "accepted"
    case _: Rejected => "rejected"
    case _ => throw new IllegalArgumentException("unsupported host command")
  }
  override def toBinary(value: AnyRef): Array[Byte] = {
    val bytes = new java.io.ByteArrayOutputStream()
    val out = new java.io.DataOutputStream(bytes)
    value match {
      case StartRun(id, state: RemoteFixtureState, reply) =>
        out.writeUTF(id); out.writeLong(state.offset); out.writeInt(state.groups.size)
        state.groups.foreach(out.writeUTF)
        out.writeUTF(refs.toSerializationFormat(reply))
      case Accepted(id) => out.writeUTF(id)
      case Rejected(id, reason) => out.writeUTF(id); out.writeUTF(reason)
      case _ => throw new IllegalArgumentException("host codec requires RemoteFixtureState")
    }
    out.close()
    bytes.toByteArray
  }
  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    val in = new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes))
    try manifest match {
      case "start" =>
        val id = in.readUTF(); val offset = in.readLong(); val size = in.readInt()
        require(size >= 0 && size <= 100)
        val groups = Vector.fill(size)(in.readUTF())
        StartRun(id, RemoteFixtureState(offset, groups), refs.resolveActorRef[RunReply](in.readUTF()))
      case "accepted" => Accepted(in.readUTF())
      case "rejected" => Rejected(in.readUTF(), in.readUTF())
      case _ => throw new IllegalArgumentException("unsupported host manifest")
    } finally in.close()
  }
}

class RemoteCommandCompatibilitySpec extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
  akka.actor.allow-java-serialization = off
  akka.actor.serializers.host-command = "net.imadz.monarch.persistence.HostCommandSerializer"
  akka.actor.serialization-bindings {
    "net.imadz.monarch.persistence.PersistentMonarch$StartRun" = host-command
    "net.imadz.monarch.persistence.PersistentMonarch$RunReply" = host-command
  }
""")) with AnyWordSpecLike with Matchers {
  "Generic remote command envelopes" should {
    "roundtrip representative host state and a usable reply ActorRef through explicit host bindings" in {
      val serialization = SerializationExtension(system.classicSystem)
      val replies = testKit.createTestProbe[RunReply]()
      val original = StartRun("remote", RemoteFixtureState(123L, Vector("retry/1", "hold/2")), replies.ref)
      val serializer = serialization.findSerializerFor(original)
      serializer.getClass shouldBe classOf[HostCommandSerializer]
      val decoded = serialization.deserialize(serialization.serialize(original).get, serializer.identifier,
        Serializers.manifestFor(serializer, original)).get.asInstanceOf[StartRun[RemoteFixtureState]]
      decoded shouldBe original
      Vector[RunReply](Accepted("remote"), Rejected("remote", "already started")).foreach { reply =>
        val s = serialization.findSerializerFor(reply)
        val restored = serialization.deserialize(serialization.serialize(reply).get, s.identifier,
          Serializers.manifestFor(s, reply)).get.asInstanceOf[RunReply]
        restored shouldBe reply
        decoded.replyTo ! restored
        replies.expectMessage(reply)
      }
    }
  }
}
