package net.imadz.monarch.persistence.workflow

/** Pure immutable data and policies: this package deliberately has no Akka dependency. */
final case class Item(sequence: Long, itemId: String, payload: Vector[Byte], compatibilityKey: String,
    area: String, executions: Map[String, Int] = Map.empty, lineage: Vector[String] = Vector.empty,
    carried: Vector[ResourceClaim] = Vector.empty) {
  def execution: Int = executions.getOrElse(area, 0)
}
final case class ResourceClaim(reservationId: String, ownerOperationKey: String, token: String, units: Map[String, Int])
final case class BatchManifest(jobId: String, batchId: String, area: String, policyVersion: String,
    items: Vector[Item], sealedAt: Long) {
  def operationKey: String = batchId + "/execute"
  def reservationId: String = batchId + "/resources"
  def deliveryId: String = batchId + "/delivery"
  def runId: String = batchId + "/run"
}

sealed trait BatchPolicy {
  def maxItems: Int
  def minItems: Int
  def maxWaitMillis: Long
  def allowPartialOnClose: Boolean
  final def ready(count: Int, firstAt: Long, now: Long, closed: Boolean, pressure: Boolean): Boolean =
    count >= maxItems || (count >= minItems && (now - firstAt >= maxWaitMillis || pressure || closed)) ||
      (closed && allowPartialOnClose && count > 0)
}
object BatchPolicy {
  final case class CountOrWait(maxItems: Int, maxWaitMillis: Long, minItems: Int = 1,
      allowPartialOnClose: Boolean = true) extends BatchPolicy {
    require(maxItems > 0 && minItems > 0 && minItems <= maxItems && maxWaitMillis >= 0,
      "invalid batch size or window")
  }
  val single: BatchPolicy = CountOrWait(1, 0L)
  def countOrWait(maxItems: Int, maxWaitMillis: Long): BatchPolicy = CountOrWait(maxItems, maxWaitMillis)
  /** Compatibility is always frozen on Item; hard minimum cannot be bypassed by timeout/EOF. */
  def compatible(maxItems: Int, minItems: Int, maxWaitMillis: Long): BatchPolicy =
    CountOrWait(maxItems, maxWaitMillis, minItems, allowPartialOnClose = false)
}

final case class AreaDefinition(id: String, policyVersion: String, batch: BatchPolicy,
    resources: Map[String, Int], parallelism: Int = 1, maxExecutions: Int = 3) {
  require(id.nonEmpty && policyVersion.nonEmpty, "area identity/version required")
  require(parallelism > 0 && maxExecutions > 0, "area execution limits must be positive")
  require(resources.nonEmpty && resources.forall { case (id, count) => id.nonEmpty && count > 0 }, "positive resource bundle required")
}

sealed trait ItemDisposition
object ItemDisposition {
  final case class Completed(result: Vector[Byte]) extends ItemDisposition
  final case class Failed(reason: String) extends ItemDisposition
  final case class Retry(delayMillis: Long) extends ItemDisposition
  final case class Route(area: String, compatibilityKey: String, delayMillis: Long = 0L) extends ItemDisposition
  /** Unknown is not a failure or a retry. Resume only through a registered verification route. */
  final case class Hold(reason: String) extends ItemDisposition
}
final case class ItemDecision(itemId: String, disposition: ItemDisposition)
sealed trait ResourceDisposition { def proof: Vector[Byte] }
object ResourceDisposition {
  final case class Release(proof: Vector[Byte]) extends ResourceDisposition
  /** Retain only the occupied units; a future batch receives this claim in its manifest.
    * Every item must route together; callers cannot split a physical occupancy claim.
    */
  final case class Carry(reservationId: String, ownerOperationKey: String, units: Map[String, Int],
      proof: Vector[Byte]) extends ResourceDisposition
}
final case class BatchCompletion(decisions: Vector[ItemDecision], resource: ResourceDisposition, retainCarried: Boolean = false)

object BatchConservation {
  def validate(manifest: BatchManifest, completion: BatchCompletion, areas: Map[String, AreaDefinition],
      maxTransitions: Int): Option[String] = {
    val expected = manifest.items.map(_.itemId)
    val received = completion.decisions.map(_.itemId)
    if (expected.distinct != expected || received.distinct != received || expected.toSet != received.toSet)
      Some("result must cover every manifest item exactly once, without foreign ids")
    else if (completion.retainCarried && (completion.decisions.exists(!_.disposition.isInstanceOf[ItemDisposition.Route]) ||
        completion.decisions.map(_.disposition).collect { case r: ItemDisposition.Route => (r.area, r.compatibilityKey, r.delayMillis) }.distinct.size != 1 ||
        !completion.resource.isInstanceOf[ResourceDisposition.Release]))
      Some("retained incoming occupancy requires all items to route together, without adding another claim")
    else completion.decisions.iterator.flatMap { decision =>
      val item = manifest.items.find(_.itemId == decision.itemId).get
      decision.disposition match {
        case ItemDisposition.Retry(delay) if delay < 0 || item.execution + 1 >= areas(item.area).maxExecutions =>
          Some("retry delay or per-item execution budget exceeded")
        case ItemDisposition.Route(target, key, delay) if !areas.contains(target) || key.isEmpty || delay < 0 ||
            item.executions.getOrElse(target, 0) + (if (target == item.area) 1 else 0) >=
              areas.get(target).map(_.maxExecutions).getOrElse(0) =>
          Some("route target, key, delay or per-item execution budget invalid")
        case _: ItemDisposition.Retry | _: ItemDisposition.Route if item.lineage.size + 1 >= maxTransitions =>
          Some("per-item transition budget exceeded")
        case ItemDisposition.Hold(reason) if reason.isEmpty => Some("held item requires a reason")
        case _ => None
      }
    }.take(1).toVector.headOption.orElse(completion.resource match {
      case ResourceDisposition.Carry(id, owner, units, _) =>
        val routes = completion.decisions.map(_.disposition).collect { case r: ItemDisposition.Route => r }
        if (id.isEmpty || owner.isEmpty || units.isEmpty || units.exists(_._2 <= 0) || routes.size != expected.size ||
            routes.map(r => (r.area, r.compatibilityKey, r.delayMillis)).distinct.size != 1)
          Some("carried occupancy requires all items to route together to one compatible target") else None
      case _ => None
    })
  }
}
