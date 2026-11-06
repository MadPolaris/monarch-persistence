package net.imadz.monarch.persistence.resource

import com.typesafe.config.Config
import net.imadz.monarch.persistence.ProtocolLimits

/** Lifetime bounds, including released/transferred IDs, not an expiring dedup cache.
  * A full domain rejects new IDs (including transfer destinations); existing claims
  * can still be queried/released. Retire the domain only after external reconciliation.
  * Aggregate state bytes can exhaust admission before maxReservations is reached.
  */
final case class ResourceSettings(
    maxReservations: Int = 10000,
    maxQueued: Int = 1000,
    limits: ProtocolLimits = ProtocolLimits()
) {
  require(maxReservations > 0 && maxReservations <= 10000, "maxReservations must be in 1..10000")
  require(maxQueued >= 0 && maxQueued <= 1000, "maxQueued must be in 0..1000")
  require(limits != null && limits.maxStringBytes >= 64, "resource strings must accommodate SHA-256")
}

object ResourceSettings {
  val ConfigPath: String = "monarch.resource"
  def fromConfig(config: Config): ResourceSettings = {
    val c = config.getConfig(ConfigPath)
    val l = c.getConfig("limits")
    ResourceSettings(c.getInt("max-reservations"), c.getInt("max-queued"), ProtocolLimits(
      maxRequestBytes = l.getInt("max-request-bytes"), maxStateBytes = l.getInt("max-state-bytes"),
      maxResultBytes = l.getInt("max-result-bytes"), maxEvidenceBytes = l.getInt("max-evidence-bytes"),
      maxEnvelopeBytes = l.getInt("max-envelope-bytes"), maxStringBytes = l.getInt("max-string-bytes"),
      maxPlanSteps = l.getInt("max-plan-steps"), maxMetadataEntries = l.getInt("max-metadata-entries")))
  }
}
