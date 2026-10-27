package net.imadz.monarch.persistence

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import PersistentMonarch._

/** Run with the published 0.2 jar ahead of candidate classes to capture real
  * historical class manifests and Jackson bytes, without modifying that artifact.
  */
object LegacyEventFixtureMain {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseString("""
      akka.actor.serialization-bindings {
        "net.imadz.monarch.persistence.PersistentMonarch$Event" = jackson-cbor
      }
      akka.loglevel = WARNING
    """)
    val system = ActorSystem[Nothing](Behaviors.empty, "LegacyFixtures", config)
    try {
      val serialization = SerializationExtension(system.classicSystem)
      val events: Vector[Event] = Vector(RunStarted("legacy-fixture", Vector("a#0", "b#1"), Array.emptyByteArray),
        StageDone("a#0", 10L, Map("artifact" -> "root"), "a".getBytes("UTF-8")),
        RunCompleted("legacy-fixture"), RunFailedEvt("a#0", "failure"))
      val base64 = java.util.Base64.getEncoder
      events.foreach { event =>
        val serializer = serialization.findSerializerFor(event)
        require(serializer.getClass.getName.endsWith("JacksonCborSerializer"))
        println(s"FIXTURE ${serializer.identifier}|${Serializers.manifestFor(serializer, event)}|${base64.encodeToString(serializer.toBinary(event))}")
      }
      println("ORIGIN " + classOf[RunStarted].getProtectionDomain.getCodeSource.getLocation)
    } finally { system.terminate(); Await.result(system.whenTerminated, 10.seconds) }
  }
}
