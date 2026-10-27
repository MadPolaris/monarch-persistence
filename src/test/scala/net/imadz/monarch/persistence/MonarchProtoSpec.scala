package net.imadz.monarch.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import net.imadz.monarch.persistence.proto.EventPO
import PersistentMonarch._

class MonarchProtoSpec extends ScalaTestWithActorTestKit(
    ConfigFactory.parseString("akka.actor.allow-java-serialization = off"))
    with AnyWordSpecLike with Matchers {
  private val serialization = SerializationExtension(system.classicSystem)

  "Monarch Protobuf protocol" should {
    "read fixed bytes and manifests produced by the published 0.2 artifact" in {
      val source = scala.io.Source.fromResource("legacy-0.2-events.txt")
      val records = try source.getLines().toVector.map { line =>
        val fields = line.split("\\|", 3)
        serialization.deserialize(java.util.Base64.getDecoder.decode(fields(2)), fields(0).toInt, fields(1)).get
      } finally source.close()
      val started = records(0).asInstanceOf[RunStarted]
      started.runId shouldBe "legacy-fixture"
      started.stageCursors shouldBe Vector("a#0", "b#1")
      started.initialSnapshot.toVector shouldBe Vector.empty
      val done = records(1).asInstanceOf[StageDone]
      done.cursor shouldBe "a#0"
      done.timestamp shouldBe 10L
      done.metadata shouldBe Map("artifact" -> "root")
      done.snapshot.toVector shouldBe "a".getBytes("UTF-8").toVector
      records(2) shouldBe RunCompleted("legacy-fixture")
      records(3) shouldBe RunFailedEvt("a#0", "failure")
    }

    "roundtrip all event variants, optional codes and binary state bytes" in {
      val binary = Array[Byte](0, 1, -1, 127)
      val events: Vector[Event] = Vector(RunStarted("id", Vector("a#0"), binary),
        StageDone("a#0", Long.MaxValue, Map("artifact" -> "root"), binary),
        StageResolved("a#0", 2L, None, "REGROUP", "split", binary),
        StageResolved("a#0", 2L, Some(""), "REGROUP", "split", binary),
        RunCompleted("id"), RunFailedEvt("a#0", "reason"), RunPlanExtended(Vector("a#0", "b#1")))
      events.foreach { event =>
        val proto = MonarchProtoConverters.eventToProto(event)
        val decoded = MonarchProtoConverters.eventFromProto(EventPO.parseFrom(proto.toByteArray))
        // Case-class equality on Array[Byte] is reference equality, so compare canonical wire bytes.
        MonarchProtoConverters.eventToProto(decoded) shouldBe proto
        serialization.findSerializerFor(event).getClass shouldBe classOf[MonarchSerializer]
      }
    }

    "reject unknown versions and empty oneof instead of silently dropping history" in {
      intercept[IllegalArgumentException](MonarchProtoConverters.eventFromProto(EventPO(schemaVersion = 2)))
      intercept[IllegalArgumentException](MonarchProtoConverters.eventFromProto(EventPO(schemaVersion = 1)))
      intercept[IllegalArgumentException](new MonarchSerializer().fromBinary(Array.emptyByteArray, "future-manifest"))
    }

    "roundtrip complete durable snapshots in every run state" in {
      val states: Vector[RunState] = Vector(Idle, Executing("id", Vector("a#0"), Array[Byte](1, -1)),
        Done("id"), Failed("id", "a#0", "reason"))
      states.foreach { state =>
        val value = DurableState(state, Vector("a#0", "b#1"), Some("invalid history"))
        val proto = MonarchProtoConverters.snapshotToProto(value)
        val serializer = serialization.findSerializerFor(value)
        val decoded = serialization.deserialize(serialization.serialize(value).get, serializer.identifier,
          Serializers.manifestFor(serializer, value)).get.asInstanceOf[DurableState]
        MonarchProtoConverters.snapshotToProto(decoded) shouldBe proto
      }
    }

    "read old Jackson event records using their stored serializer id and manifest" in {
      val legacy = serialization.serializerByIdentity.values.find(_.getClass.getName.endsWith("JacksonCborSerializer")).get
      val events: Vector[Event] = Vector(RunStarted("legacy", Vector("a#0"), Array[Byte](1, 2)),
        StageDone("a#0", 10L, Map("key" -> "value"), Array[Byte](3, 4)),
        RunCompleted("legacy"), RunFailedEvt("a#0", "error"))
      events.foreach { event =>
        val bytes = legacy.toBinary(event)
        val manifest = Serializers.manifestFor(legacy, event)
        val decoded = serialization.deserialize(bytes, legacy.identifier, manifest).get.asInstanceOf[Event]
        MonarchProtoConverters.eventToProto(decoded) shouldBe MonarchProtoConverters.eventToProto(event)
        serialization.findSerializerFor(decoded).getClass shouldBe classOf[MonarchSerializer]
      }
    }
  }
}
