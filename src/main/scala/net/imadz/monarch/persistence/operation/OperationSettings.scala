package net.imadz.monarch.persistence.operation

import com.typesafe.config.Config
import net.imadz.monarch.persistence.ProtocolLimits

import scala.concurrent.duration._

/** Limits apply to a single operation. All retry/scheduling policy is frozen by
  * Prepared, so changing host defaults cannot reset a recovered operation's
  * budgets. Manual reconciliation replaces, never adds to, the query allowance.
  * It is bounded both per request and over the operation's lifetime.
  *
  * For configured overrides use fromConfig(system.settings.config); serializers
  * use that configuration too. Actor-specific limits must not exceed the system
  * serializer limits. Journal write/replay availability is not an I/O timeout:
  * Get/Ensure can themselves time out while the journal is hung.
  */
final case class OperationSettings(
    ioTimeout: FiniteDuration = 10.seconds,
    checkInterval: FiniteDuration = 1.second,
    maxChecks: Int = 8,
    manualCheckBudget: Int = 3,
    maxReconciliations: Int = 8,
    maxSubmissions: Int = 1,
    limits: ProtocolLimits = ProtocolLimits()
) {
  private[persistence] val policy: OperationPolicy = OperationPolicy(
    ioTimeout.toMillis, checkInterval.toMillis, maxChecks, manualCheckBudget,
    maxReconciliations, maxSubmissions)
  require(ioTimeout >= 1.millis && ioTimeout <= OperationSettings.MaxDelay,
    "I/O timeout must be between 1ms and 7 days")
  require(checkInterval >= 1.millis && checkInterval <= OperationSettings.MaxDelay,
    "check interval must be between 1ms and 7 days")
  require(limits != null, "protocol limits must not be null")
  require(limits.maxStringBytes >= 64, "strings must accommodate a SHA-256 digest")
  // Request + terminal (one proof) + metadata, tags and length prefixes. State
  // never stores a second copy of terminal evidence. This conservative bound
  // prevents a valid individual field from making a journal envelope unwritable.
  require(limits.maxEnvelopeBytes.toLong >= limits.maxRequestBytes.toLong +
    limits.maxResultBytes + limits.maxEvidenceBytes + 20L * limits.maxStringBytes + 1024L,
    "operation envelope must accommodate payloads, identity strings and framing")
}

object OperationSettings {
  private[operation] val MaxDelay: FiniteDuration = 7.days
  val ConfigPath: String = "monarch.operation"

  def fromConfig(config: Config): OperationSettings = {
    val c = config.getConfig(ConfigPath)
    val l = c.getConfig("limits")
    OperationSettings(
      ioTimeout = c.getDuration("io-timeout").toMillis.millis,
      checkInterval = c.getDuration("check-interval").toMillis.millis,
      maxChecks = c.getInt("max-checks"),
      manualCheckBudget = c.getInt("manual-check-budget"),
      maxReconciliations = c.getInt("max-reconciliations"),
      maxSubmissions = c.getInt("max-submissions"),
      limits = ProtocolLimits(
        maxRequestBytes = l.getInt("max-request-bytes"),
        maxStateBytes = l.getInt("max-state-bytes"),
        maxResultBytes = l.getInt("max-result-bytes"),
        maxEvidenceBytes = l.getInt("max-evidence-bytes"),
        maxEnvelopeBytes = l.getInt("max-envelope-bytes"),
        maxStringBytes = l.getInt("max-string-bytes"),
        maxPlanSteps = l.getInt("max-plan-steps"),
        maxMetadataEntries = l.getInt("max-metadata-entries")))
  }
}

private[persistence] final case class OperationPolicy(
    ioTimeoutMillis: Long,
    checkIntervalMillis: Long,
    maxChecks: Int,
    manualCheckBudget: Int,
    maxReconciliations: Int,
    maxSubmissions: Int
) {
  require(ioTimeoutMillis > 0 && ioTimeoutMillis <= OperationSettings.MaxDelay.toMillis,
    "invalid recorded I/O timeout")
  require(checkIntervalMillis > 0 && checkIntervalMillis <= OperationSettings.MaxDelay.toMillis,
    "invalid recorded check interval")
  require(maxChecks > 0 && manualCheckBudget > 0 && maxSubmissions > 0 && maxReconciliations >= 0,
    "operation budgets must be positive (reconciliation count may be zero)")
  val totalCheckBudget: Long = maxChecks.toLong + manualCheckBudget.toLong * maxReconciliations
  require(totalCheckBudget <= Int.MaxValue, "operation query budget overflows its durable counter")
  require(maxSubmissions.toLong <= totalCheckBudget + 1L,
    "every resubmission requires a budgeted authorizing query")
}
