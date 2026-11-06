package net.imadz.monarch.persistence.delivery

import com.typesafe.config.Config
import net.imadz.monarch.persistence.ProtocolLimits

import scala.concurrent.duration._

/** Policy (including payload limits) is frozen at acceptance. Each automatic or
  * operator round has maxPolls queries. There are at most three operator rounds;
  * recreation also consumes a durable, finite same-child ensure budget.
  * One actor owns one delivery; the host bounds the number of active entities.
  */
final case class DeliverySettings(
    queryTimeout: FiniteDuration = 5.seconds,
    pollInterval: FiniteDuration = 1.second,
    maxPolls: Int = 100,
    limits: ProtocolLimits = ProtocolLimits()
) {
  require(queryTimeout >= 1.millis && queryTimeout <= DeliverySettings.MaxDelay,
    "delivery query timeout must be between 1ms and 7 days")
  require(pollInterval >= 1.millis && pollInterval <= DeliverySettings.MaxDelay,
    "delivery poll interval must be between 1ms and 7 days")
  private[persistence] val policy: DeliveryPolicy = DeliveryPolicy(queryTimeout.toMillis,
    pollInterval.toMillis, maxPolls, DeliverySettings.MaxReconciliations, limits)
}

object DeliverySettings {
  val ConfigPath: String = "monarch.delivery"
  private[delivery] val MaxDelay: FiniteDuration = 7.days
  private[persistence] val MaxReconciliations: Int = 3

  def fromConfig(config: Config): DeliverySettings = {
    val c = config.getConfig(ConfigPath)
    val l = c.getConfig("limits")
    DeliverySettings(c.getDuration("query-timeout").toMillis.millis,
      c.getDuration("poll-interval").toMillis.millis, c.getInt("max-polls"),
      ProtocolLimits(maxRequestBytes = l.getInt("max-request-bytes"),
        maxStateBytes = l.getInt("max-state-bytes"), maxResultBytes = l.getInt("max-result-bytes"),
        maxEvidenceBytes = l.getInt("max-evidence-bytes"), maxEnvelopeBytes = l.getInt("max-envelope-bytes"),
        maxStringBytes = l.getInt("max-string-bytes"), maxPlanSteps = l.getInt("max-plan-steps"),
        maxMetadataEntries = l.getInt("max-metadata-entries")))
  }
}

private[persistence] final case class DeliveryPolicy(queryTimeoutMillis: Long, pollIntervalMillis: Long,
    maxPolls: Int, maxReconciliations: Int, limits: ProtocolLimits) {
  require(queryTimeoutMillis > 0L && queryTimeoutMillis <= DeliverySettings.MaxDelay.toMillis,
    "invalid recorded delivery query timeout")
  require(pollIntervalMillis > 0L && pollIntervalMillis <= DeliverySettings.MaxDelay.toMillis,
    "invalid recorded delivery poll interval")
  require(maxPolls > 0 && maxReconciliations >= 0 && maxReconciliations <= DeliverySettings.MaxReconciliations,
    "invalid delivery polling/reconciliation budget")
  val totalPollBudget: Long = maxPolls.toLong * (maxReconciliations.toLong + 1L)
  require(totalPollBudget < Int.MaxValue, "delivery call budget overflows its durable counter")
  val maxStarts: Int = (totalPollBudget + 1L).toInt
  require(limits != null && limits.maxStringBytes >= 64, "delivery strings must accommodate SHA-256 digests")
  // Initial child state and terminal result can coexist in a snapshot. Reserve
  // identity/diagnostic/ActorRef strings and framing, not only individual bytes.
  require(limits.maxEnvelopeBytes.toLong >= limits.maxRequestBytes.toLong + limits.maxStateBytes +
    limits.maxResultBytes + 32L * limits.maxStringBytes + 2048L,
    "delivery envelope must accommodate request, child state, terminal result and framing")
}
