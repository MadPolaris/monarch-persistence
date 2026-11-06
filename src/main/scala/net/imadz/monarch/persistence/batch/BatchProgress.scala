package net.imadz.monarch.persistence.batch

import net.imadz.monarch.persistence.workflow._

/** Pure progression used both to produce settlement records and to reject forged replay. */
private[persistence] object BatchProgress {
  private val allowed = Map(
    "Reserving" -> Set("Reserving", "Preparing", "Blocked"),
    "Preparing" -> Set("Delivering", "Blocked"),
    "Delivering" -> Set("Delivering", "WaitingChild", "ReleasingCarried", "Blocked"),
    "WaitingChild" -> Set("WaitingChild", "ReleasingCarried", "Blocked"),
    "ReconcilingChild" -> Set("ReconcilingChild", "WaitingChild", "ReleasingCarried", "Blocked"),
    "ReleasingCarried" -> Set("ReleasingCarried", "Releasing", "Blocked"),
    "Releasing" -> Set("Releasing", "Settling", "Blocked"),
    "Settling" -> Set("Blocked"))
  def validAdvance(old: ActiveBatch, next: ActiveBatch): Boolean = {
    val resume = old.resumePhase.map { saved =>
      if (old.delivery.nonEmpty && old.completion.isEmpty) "ReconcilingChild" else saved
    }
    val transition = if (old.phase == "Blocked") resume.contains(next.phase) && next.resumePhase.isEmpty &&
      next.reconciliations == old.reconciliations + 1 && next.calls == 0 && next.reason.isEmpty
    else allowed.getOrElse(old.phase, Set.empty).contains(next.phase) && next.reconciliations == old.reconciliations &&
      next.calls >= old.calls && next.calls <= old.calls + 1 &&
      (if (next.phase == "Blocked") next.resumePhase.contains(old.phase) && next.reason.nonEmpty else next.resumePhase.isEmpty)
    val additions = old.grant.forall(g => next.grant.contains(g)) && old.delivery.forall(d => next.delivery.contains(d)) &&
      old.completion.forall(c => next.completion.contains(c)) && old.carry.forall(c => next.carry.contains(c)) &&
      (old.grant.nonEmpty || next.grant.isEmpty || old.phase == "Reserving" && next.phase == "Preparing") &&
      (old.delivery.nonEmpty || next.delivery.isEmpty || old.phase == "Preparing" && next.phase == "Delivering") &&
      (old.completion.nonEmpty || next.completion.isEmpty || Set("Delivering", "WaitingChild", "ReconcilingChild")(old.phase) && next.phase == "ReleasingCarried") &&
      (old.carry.nonEmpty || next.carry.isEmpty || old.phase == "Releasing" && next.phase == "Settling")
    val releaseCount = next.releasedCarried >= old.releasedCarried && next.releasedCarried <= old.releasedCarried + 1 &&
      (next.releasedCarried == old.releasedCarried || old.phase == "ReleasingCarried" && next.phase == "ReleasingCarried")
    val allCarriedReleased = next.phase != "Releasing" || next.completion.exists(_.retainCarried) ||
      next.releasedCarried == next.manifest.items.flatMap(_.carried).distinct.size
    val transferRecorded = next.phase != "Settling" || next.completion.forall(_.resource match {
      case _: ResourceDisposition.Carry => next.carry.nonEmpty
      case _: ResourceDisposition.Release => next.carry.isEmpty
    })
    old.manifest == next.manifest && transition && additions && releaseCount && allCarriedReleased && transferRecorded
  }

  def validVerification(old: ItemRecord, next: ItemRecord): Boolean =
    old.phase == "Held" && next.phase == "Pending" && next.reason.isEmpty && next.batchId.isEmpty &&
      next.item.copy(area = old.item.area, compatibilityKey = old.item.compatibilityKey) == old.item &&
      next.inputDigest == old.inputDigest && next.result == old.result && next.acceptedAt >= old.acceptedAt &&
      next.dueAt >= next.acceptedAt

  def settle(batch: ActiveBatch, previous: Map[Long, ItemRecord], now: Long): Vector[ItemRecord] = {
    val completion = batch.completion.get
    val decisions = completion.decisions.map(d => d.itemId -> d.disposition).toMap
    batch.manifest.items.map { item =>
      val old = previous(item.sequence)
      val executed = item.copy(executions = item.executions.updated(item.area, item.execution + 1),
        lineage = item.lineage :+ batch.manifest.batchId,
        carried = if (completion.retainCarried) item.carried else batch.carry.toVector)
      decisions(item.itemId) match {
        case ItemDisposition.Completed(result) => old.copy(item = executed.copy(payload = Vector.empty, carried = Vector.empty),
          phase = "Succeeded", batchId = None, result = result, reason = None)
        case ItemDisposition.Failed(reason) => old.copy(item = executed.copy(payload = Vector.empty, carried = Vector.empty),
          phase = "Failed", batchId = None, reason = Some(reason))
        case ItemDisposition.Hold(reason) => old.copy(item = executed, phase = "Held", batchId = None, reason = Some(reason))
        case ItemDisposition.Retry(delay) => old.copy(item = executed, phase = "Pending", batchId = None,
          acceptedAt = now, dueAt = Math.addExact(now, delay), reason = None)
        case ItemDisposition.Route(target, key, delay) => old.copy(item = executed.copy(area = target, compatibilityKey = key),
          phase = "Pending", batchId = None, acceptedAt = now, dueAt = Math.addExact(now, delay), reason = None)
      }
    }
  }
  def validSettlement(batch: ActiveBatch, records: Vector[ItemRecord], previous: Map[Long, ItemRecord]): Boolean = {
    if (batch.phase != "Settling" || batch.completion.isEmpty || records.map(_.item.sequence).distinct.size != records.size ||
        records.map(_.item.sequence).toSet != batch.manifest.items.map(_.sequence).toSet) false
    else {
      val pendingTimes = records.filter(_.phase == "Pending").map(_.acceptedAt).distinct
      val now = pendingTimes.headOption.getOrElse(0L)
      pendingTimes.size <= 1 && now >= 0 && settle(batch, previous, now).sortBy(_.item.sequence) == records.sortBy(_.item.sequence)
    }
  }
}
