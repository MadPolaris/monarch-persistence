package net.imadz.monarch.persistence.batch

import akka.actor.Cancellable
import akka.actor.typed.{ActorRef, Behavior, BehaviorInterceptor, BehaviorSignalInterceptor, PostStop, PreRestart, Signal, TypedActorContext}
import akka.actor.typed.scaladsl.{AskPattern, Behaviors}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.util.{ByteString, Timeout}
import net.imadz.monarch.RunLease
import net.imadz.monarch.persistence.delivery.DeliveryProtocol
import net.imadz.monarch.persistence.resource.ResourceProtocol
import net.imadz.monarch.persistence.workflow._

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** A bounded durable job. It owns acceptance, sealing and disposition, while the
  * existing Monarch owns stage execution and PersistentInbox owns child startup.
  * Credit follows an item through all queues/children until settlement or hold.
  */
object BatchCoordinator {
  import BatchProtocol._

  def behavior(persistenceId: PersistenceId, jobId: String, definition: BatchDefinition,
      resources: ActorRef[ResourceProtocol.Command], inbox: String => ActorRef[DeliveryProtocol.Command],
      execution: BatchExecutionPort)(implicit ec: ExecutionContext): Behavior[Command] = Behaviors.setup { ctx =>
    import AskPattern._
    val settings = definition.settings
    val limits = settings.limits
    require(limits.text("jobId", jobId).isEmpty, "invalid jobId")
    implicit val scheduler: akka.actor.typed.Scheduler = ctx.system.scheduler
    implicit val timeout: Timeout = Timeout(settings.callTimeoutMillis.millis)
    val timer = new AtomicReference[Cancellable]()
    val lease = RunLease.acquire("monarch:batch:" + persistenceId.id, () => Option(timer.getAndSet(null)).foreach(_.cancel()))
    val generation = lease.generation
    var token = 0L
    var callId = 0L
    var inFlight = Map.empty[String, (Long, Cancellable)]
    var localProblem: Option[String] = None
    var lastDriven: Option[String] = None
    def close(): Unit = {
      lease.revoke()
      inFlight.valuesIterator.foreach(_._2.cancel())
      inFlight = Map.empty
    }
    def issue(state: BatchState): Option[String] = state.invalid.orElse(localProblem)
      .orElse(if (state.definitionDigest.exists(_ != definition.fingerprint)) Some("workflow definition/settings do not match the frozen job") else None)
      .orElse(if (state.jobId.exists(_ != jobId)) Some("job identity differs from this entity") else None)
      .orElse(if (!lease.isFresh) Some("job owner revoked") else None)
    def active(state: BatchState): Vector[ItemRecord] = state.records.valuesIterator.filter(r =>
      r.phase != "Succeeded" && r.phase != "Failed").toVector
    def occupiedBytes(state: BatchState): Long = active(state).iterator.map(_.item.payload.size.toLong).sum
    def wake(delay: Long = 0L): Unit = if (lease.isFresh) {
      token += 1
      val next = token
      Option(timer.getAndSet(scheduler.scheduleOnce(delay.millis, new Runnable {
        override def run(): Unit = ctx.self ! Drive(generation, next)
      })(ctx.executionContext))).foreach(_.cancel())
    }
    def view(state: BatchState, after: Long, count: Int): View = {
      val all = state.records.valuesIterator.toVector.sortBy(_.item.sequence)
      val unclosed = active(state)
      val blocking = issue(state).orElse(state.batches.valuesIterator.flatMap(_.reason).take(1).toVector.headOption)
        .orElse(if (all.exists(_.phase == "Held")) Some("held items require evidence or a registered verification route") else None)
      val drained = state.closed && unclosed.isEmpty && state.batches.isEmpty
      val phase = if (issue(state).nonEmpty) "RecoveryBlocked" else if (drained) "Drained"
        else if (state.closed && blocking.nonEmpty) "ClosingBlocked" else if (blocking.nonEmpty) "Blocked"
        else if (state.closed && state.batches.isEmpty && all.exists(_.phase == "Pending")) "ClosingBlocked"
        else if (state.closed) "Closing" else "Open"
      val selected = all.filter(_.item.sequence > after).take(count)
      View(jobId, phase, state.closed, state.nextSequence, all.count(_.phase == "Succeeded"), all.count(_.phase == "Failed"),
        all.count(_.phase == "Held"), unclosed.size, occupiedBytes(state),
        state.batches.values.toVector.sortBy(_.manifest.batchId).map { b => BatchView(b.manifest.batchId, b.manifest.area,
          b.phase, b.manifest.items.map(_.itemId), b.grant.map(_.reservationId), b.delivery.map(_.deliveryId), b.reason) },
        selected.map(r => ItemView(r.item.sequence, r.item.itemId, r.item.area, r.phase, r.item.executions,
          r.reason, ByteString(r.result.toArray))), all.count(_.item.sequence > after) > selected.size,
        blocking.orElse(if (phase == "ClosingBlocked") Some("tail batch cannot yet satisfy its physical minimum/compatibility") else None))
    }
    def safe[A](body: => Either[String, A]): Either[String, A] =
      try body catch { case NonFatal(ex) => Left(limits.diagnostic(Option(ex.getMessage).getOrElse("host policy failed"))) }
    def changed(batch: ActiveBatch)(after: => Unit = wake()): Effect[BatchEvent, BatchState] = {
      val valid = try { BatchCodec.activeToProto(batch); Right(()) } catch { case NonFatal(ex) => Left(ex) }
      valid match {
        case Right(_) => Effect.persist(BatchEvent.Advanced(batch)).thenRun(_ => after)
        case Left(ex) =>
          localProblem = Some(limits.diagnostic("unpersistable batch transition: " + ex.getMessage))
          Effect.none
      }
    }
    def block(batch: ActiveBatch, reason: String): Effect[BatchEvent, BatchState] =
      changed(batch.copy(phase = "Blocked", resumePhase = batch.resumePhase.orElse(Some(batch.phase)),
        reason = Some(limits.diagnostic(reason))))()

    def call(batch: ActiveBatch)(body: => Future[CallResult]): Effect[BatchEvent, BatchState] = {
      if (batch.calls >= settings.maxBatchCalls) block(batch, "automatic batch query budget exhausted")
      else {
        val next = batch.copy(calls = batch.calls + 1, nextDue = System.currentTimeMillis() + settings.pollIntervalMillis)
        changed(next) {
          if (lease.isFresh) {
            callId += 1
            val id = callId
            val watch = scheduler.scheduleOnce(settings.callTimeoutMillis.millis, new Runnable {
              override def run(): Unit = ctx.self ! CallTimeout(generation, batch.manifest.batchId, id)
            })(ctx.executionContext)
            inFlight += batch.manifest.batchId -> (id -> watch)
            val future = try body catch { case NonFatal(ex) => Future.failed(ex) }
            future.onComplete(result => ctx.self ! Called(generation, batch.manifest.batchId, id, result))(ctx.executionContext)
            wake(settings.pollIntervalMillis)
          }
        }
      }
    }
    def resource(batch: ActiveBatch)(make: ActorRef[ResourceProtocol.Reply] => ResourceProtocol.Command): Effect[BatchEvent, BatchState] =
      call(batch)(resources.ask[ResourceProtocol.Reply](make).map(ResourceResult)(ctx.executionContext))
    def delivery(batch: ActiveBatch, ensure: Boolean): Effect[BatchEvent, BatchState] = {
      val request = batch.delivery.get
      call(batch)(inbox(request.deliveryId).ask[DeliveryProtocol.Reply](reply =>
        if (ensure) DeliveryProtocol.Deliver(request, reply) else DeliveryProtocol.Get(request.deliveryId, reply))
        .map(DeliveryResult)(ctx.executionContext))
    }
    def carried(batch: ActiveBatch): Vector[ResourceClaim] = batch.manifest.items.flatMap(_.carried).distinct.sortBy(_.reservationId)
    def applyResults(batch: ActiveBatch, state: BatchState): Effect[BatchEvent, BatchState] = {
      val records = BatchProgress.settle(batch, state.records, System.currentTimeMillis())
      Effect.persist(BatchEvent.Settled(batch.manifest.batchId, records)).thenRun(_ => wake())
    }
    def proceed(batch: ActiveBatch, state: BatchState): Effect[BatchEvent, BatchState] = batch.phase match {
      case "Reserving" => resource(batch)(reply => ResourceProtocol.Reserve(ResourceProtocol.Request(
        batch.manifest.reservationId, batch.manifest.operationKey, definition.byId(batch.manifest.area).resources), reply))
      case "Preparing" => safe(execution.prepare(batch.manifest, batch.grant.get)) match {
        case Left(reason) => block(batch, reason)
        case Right(submission) if submission.runId != batch.manifest.runId || submission.validate(limits).nonEmpty =>
          block(batch, "host prepared an invalid or unstable child run submission")
        case Right(submission) =>
          val request = DeliveryProtocol.Request(batch.manifest.deliveryId, jobId, batch.manifest.area, submission,
            ByteString(submission.fingerprint))
          changed(batch.copy(phase = "Delivering", delivery = Some(request)))()
      }
      case "Delivering" => delivery(batch, ensure = true)
      case "WaitingChild" => delivery(batch, ensure = false)
      case "ReconcilingChild" =>
        val request = batch.delivery.get
        // A lost delivery ACK does not prove that the inbox accepted the request.
        // First reattach the exact durable envelope; only then replenish a blocked
        // child's bounded query round. Neither action creates a different child.
        call(batch) {
          val target = inbox(request.deliveryId)
          target.ask[DeliveryProtocol.Reply](reply => DeliveryProtocol.Deliver(request, reply)).flatMap {
            case DeliveryProtocol.Current(v) if v.deliveryId == request.deliveryId &&
                v.requestDigest.contains(request.fingerprint) && v.childRunId.contains(request.child.runId) && v.phase == "Blocked" =>
              target.ask[DeliveryProtocol.Reply](reply => DeliveryProtocol.Reconcile(request.deliveryId, reply))
            case other => Future.successful(other)
          }(ctx.executionContext).map(DeliveryResult)(ctx.executionContext)
        }
      case "ReleasingCarried" if batch.completion.exists(_.retainCarried) => changed(batch.copy(phase = "Releasing"))()
      case "ReleasingCarried" if batch.releasedCarried < carried(batch).size =>
        val claim = carried(batch)(batch.releasedCarried)
        resource(batch)(reply => ResourceProtocol.Release(claim.reservationId, claim.token,
          ResourceProtocol.Proof(claim.ownerOperationKey, ByteString(batch.completion.get.resource.proof.toArray)), reply))
      case "ReleasingCarried" => changed(batch.copy(phase = "Releasing"))()
      case "Releasing" => batch.completion.get.resource match {
        case ResourceDisposition.Release(proof) => resource(batch)(reply => ResourceProtocol.Release(batch.grant.get.reservationId,
          batch.grant.get.token, ResourceProtocol.Proof(batch.manifest.operationKey, ByteString(proof.toArray)), reply))
        case ResourceDisposition.Carry(id, owner, units, proof) => resource(batch)(reply => ResourceProtocol.Transfer(
          batch.grant.get.reservationId, batch.grant.get.token, id, owner, units,
          ResourceProtocol.Proof(batch.manifest.operationKey, ByteString(proof.toArray)), reply))
      }
      case "Settling" => applyResults(batch, state)
      case _ => Effect.none
    }
    def onResult(batch: ActiveBatch, result: CallResult): Effect[BatchEvent, BatchState] = result match {
      case ResourceResult(ResourceProtocol.Granted(grant)) if batch.phase == "Reserving" =>
        if (grant.reservationId != batch.manifest.reservationId || grant.ownerOperationKey != batch.manifest.operationKey ||
            grant.units != definition.byId(batch.manifest.area).resources) block(batch, "resource grant does not match sealed batch")
        else changed(batch.copy(phase = "Preparing", grant = Some(grant), reason = None))()
      case ResourceResult(ResourceProtocol.Queued(id)) if batch.phase == "Reserving" && id == batch.manifest.reservationId =>
        changed(batch.copy(nextDue = System.currentTimeMillis() + settings.pollIntervalMillis))()
      case ResourceResult(ResourceProtocol.Released(id)) if batch.phase == "ReleasingCarried" &&
          carried(batch).lift(batch.releasedCarried).exists(_.reservationId == id) =>
        changed(batch.copy(releasedCarried = batch.releasedCarried + 1))()
      case ResourceResult(ResourceProtocol.Released(id)) if batch.phase == "Releasing" && id == batch.grant.get.reservationId &&
          batch.completion.exists(_.resource.isInstanceOf[ResourceDisposition.Release]) =>
        changed(batch.copy(phase = "Settling"))()
      case ResourceResult(ResourceProtocol.Granted(grant)) if batch.phase == "Releasing" => batch.completion.get.resource match {
        case ResourceDisposition.Carry(id, owner, units, _) if grant.reservationId == id && grant.ownerOperationKey == owner && grant.units == units =>
          changed(batch.copy(phase = "Settling", carry = Some(ResourceClaim(id, owner, grant.token, units))))()
        case _ => block(batch, "resource occupancy transfer did not match committed intent")
      }
      case ResourceResult(ResourceProtocol.Unknown(_, reason)) => block(batch, reason)
      case ResourceResult(ResourceProtocol.Rejected(_, reason)) => block(batch, reason)
      case DeliveryResult(DeliveryProtocol.Current(reply)) if batch.delivery.exists(d => reply.deliveryId == d.deliveryId &&
          reply.requestDigest.contains(d.fingerprint) && reply.childRunId.contains(d.child.runId)) => reply.phase match {
        case "Completed" if reply.result.nonEmpty => safe(execution.classify(batch.manifest, reply.result.get)) match {
          case Left(reason) => block(batch, reason)
          case Right(completion) =>
            val problem = safe {
              BatchCodec.completionToBytes(completion)
              val preview = batch.copy(phase = "Settling", completion = Some(completion),
                carry = completion.resource match {
                  case ResourceDisposition.Carry(id, owner, units, _) => Some(ResourceClaim(id, owner, "preview-token", units))
                  case _ => None
                })
              val previous = batch.manifest.items.map(i => i.sequence -> ItemRecord(i, "0" * 64, "Running",
                batch.manifest.sealedAt, batch.manifest.sealedAt, Some(batch.manifest.batchId))).toMap
              BatchProgress.settle(preview, previous, System.currentTimeMillis()).foreach(BatchCodec.recordToProto)
              Right(())
            }.left.toOption.orElse(BatchConservation.validate(batch.manifest, completion, definition.byId, settings.maxTransitions))
              .orElse(if (completion.decisions.exists {
                case ItemDecision(_, ItemDisposition.Completed(result)) => result.size > settings.maxResultBytes
                case _ => false
              }) Some("item result exceeds declared capacity") else None)
            problem match {
              case Some(reason) => block(batch, reason)
              case None => changed(batch.copy(phase = "ReleasingCarried", completion = Some(completion), reason = None))()
            }
        }
        // Inbox Reconciled commits a query grant while remaining Blocked until
        // its first authoritative child observation. That ACK is not a refusal.
        case "Blocked" if reply.reconciling => changed(batch.copy(phase = "WaitingChild",
          nextDue = System.currentTimeMillis() + settings.pollIntervalMillis, reason = None))()
        case "Accepted" | "Starting" | "Running" => changed(batch.copy(phase = "WaitingChild",
          nextDue = System.currentTimeMillis() + settings.pollIntervalMillis, reason = None))()
        case _ => block(batch, reply.reason.getOrElse("child run is not authoritatively completed"))
      }
      case DeliveryResult(DeliveryProtocol.Rejected(_, reason)) => block(batch, reason)
      case _ => block(batch, "unexpected resource or delivery response identity")
    }

    val handler: (BatchState, Command) => Effect[BatchEvent, BatchState] = { (state, command) =>
      command match {
        case Get(replyTo, after, count) =>
          if (count <= 0 || count > 100) replyTo ! Rejected("query page must contain 1..100 items")
          else replyTo ! Current(view(state, after, count))
          Effect.none
        case offer: Offer =>
          val error = limits.text("itemId", offer.itemId).orElse(limits.text("compatibilityKey", offer.compatibilityKey))
            .orElse(limits.text("area", offer.area)).orElse(limits.bytes("payload", offer.payload, settings.maxItemBytes))
            .orElse(if (!definition.byId.contains(offer.area)) Some("unknown area") else None)
          if (error.nonEmpty) { offer.replyTo ! Rejected(error.get); Effect.none }
          else state.records.get(offer.sequence) match {
            case Some(record) if record.inputDigest == offer.fingerprint =>
              offer.replyTo ! Accepted(offer.sequence, offer.itemId, duplicate = true); Effect.none
            case Some(_) => offer.replyTo ! Rejected("sequence already accepted with a different immutable item"); Effect.none
            case None if issue(state).nonEmpty => offer.replyTo ! Rejected(issue(state).get); Effect.none
            case None if state.closed => offer.replyTo ! Rejected("external input is closed"); Effect.none
            case None if offer.sequence != state.nextSequence || state.records.valuesIterator.exists(_.item.itemId == offer.itemId) =>
              offer.replyTo ! Rejected("input requires the next contiguous sequence and a unique itemId"); Effect.none
            case None if state.nextSequence >= settings.maxAcceptedItems =>
              offer.replyTo ! Rejected("finite job input capacity reached; close this job and start a new identity"); Effect.none
            case None if active(state).size >= settings.maxActiveItems || occupiedBytes(state) + offer.payload.size > settings.maxActiveBytes =>
              offer.replyTo ! Backpressured("unsettled item/byte capacity reached"); wake(); Effect.none
            case None =>
              val now = System.currentTimeMillis()
              val item = Item(offer.sequence, offer.itemId, offer.payload.toVector, offer.compatibilityKey, offer.area)
              val accepted = BatchEvent.InputAccepted(ItemRecord(item, offer.fingerprint, "Pending", now, now))
              val events = if (state.jobId.isEmpty) Vector(BatchEvent.Initialized(jobId, definition.fingerprint), accepted) else Vector(accepted)
              Effect.persist(events).thenRun { _ => offer.replyTo ! Accepted(offer.sequence, offer.itemId, duplicate = false); wake() }
          }
        case CloseInput(replyTo) if issue(state).isEmpty =>
          if (state.closed) { replyTo ! Current(view(state, -1, 100)); Effect.none }
          else {
            val events = if (state.jobId.isEmpty) Vector(BatchEvent.Initialized(jobId, definition.fingerprint), BatchEvent.InputClosed)
              else Vector(BatchEvent.InputClosed)
            Effect.persist(events).thenRun(s => { replyTo ! Current(view(s, -1, 100)); wake() })
          }
        case CloseInput(replyTo) => replyTo ! Rejected(issue(state).getOrElse("job unavailable")); Effect.none
        case Reconcile(id, replyTo) => state.batches.get(id) match {
          case Some(batch) if issue(state).isEmpty && batch.phase == "Blocked" && !inFlight.contains(id) &&
              batch.reconciliations < settings.maxReconciliations =>
            val phase = if (batch.delivery.nonEmpty && batch.completion.isEmpty) "ReconcilingChild" else batch.resumePhase.getOrElse("Reserving")
            Effect.persist(BatchEvent.Advanced(batch.copy(phase = phase, resumePhase = None, reason = None, calls = 0,
              reconciliations = batch.reconciliations + 1, nextDue = 0L))).thenRun(s => { replyTo ! Current(view(s, -1, 100)); wake() })
          case _ => replyTo ! Rejected("batch is not reconcilable, is still calling, or exhausted its manual budget"); Effect.none
        }
        case VerifyItem(sequence, area, key, replyTo) => state.records.get(sequence) match {
          case Some(record) if issue(state).isEmpty && record.phase == "Held" && definition.byId.contains(area) &&
              limits.text("verification key", key).isEmpty && safe(execution.verifyRoute(record.item, area)).isRight &&
              record.item.lineage.size < settings.maxTransitions &&
              record.item.executions.getOrElse(area, 0) < definition.byId(area).maxExecutions =>
            val updated = record.copy(item = record.item.copy(area = area, compatibilityKey = key), phase = "Pending",
              reason = None, acceptedAt = System.currentTimeMillis(), dueAt = System.currentTimeMillis())
            Effect.persist(BatchEvent.Verified(updated)).thenRun(s => { replyTo ! Current(view(s, -1, 100)); wake() })
          case _ => replyTo ! Rejected("only a held item with remaining budget can enter a registered verification route"); Effect.none
        }
        case Drive(g, t) if g == generation && t == token && issue(state).isEmpty =>
          val now = System.currentTimeMillis()
          val pressure = active(state).size >= settings.maxActiveItems ||
            occupiedBytes(state) + settings.maxItemBytes > settings.maxActiveBytes
          val pending = state.records.valuesIterator.filter(_.phase == "Pending").toVector
          val groups = pending.filter(_.dueAt <= now).groupBy(r => (r.item.area, r.item.compatibilityKey, r.item.carried))
            .values.toVector.map(_.sortBy(_.item.sequence)).sortBy(_.head.item.sequence)
          val carriedPending = pending.exists(_.item.carried.nonEmpty)
          val group = if (state.batches.size >= settings.maxConcurrentBatches) None else groups.find { items =>
            val area = definition.byId(items.head.item.area)
            val claims = items.head.item.carried
            val allHolders = if (claims.isEmpty) items else pending.filter(r => r.item.carried == claims)
            state.batches.valuesIterator.count(_.manifest.area == area.id) < area.parallelism &&
              (!carriedPending || claims.nonEmpty) &&
              (claims.isEmpty || items.size == allHolders.size && items.size <= area.batch.maxItems) &&
              items.forall(r => r.item.execution < area.maxExecutions && r.item.lineage.size < settings.maxTransitions) &&
              area.batch.ready(items.size, items.map(_.acceptedAt).min, now, state.closed, pressure)
          }
          // Fill available slots before polling existing children. Continuations holding
          // physical occupancy outrank new intake even while their retry deadline is pending.
          group match {
            case Some(items) =>
              val area = definition.byId(items.head.item.area)
              val manifest = BatchManifest(jobId, s"$jobId/batch-${state.nextBatch}", area.id, area.policyVersion,
                items.take(area.batch.maxItems).map(_.item), now)
              Effect.persist(BatchEvent.Sealed(ActiveBatch(manifest))).thenRun(_ => wake())
            case None =>
              val ready = state.batches.valuesIterator.filter(b => b.phase != "Blocked" && !inFlight.contains(b.manifest.batchId) &&
                b.nextDue <= now).toVector.sortBy(_.manifest.batchId)
              val selected = ready.find(b => lastDriven.forall(b.manifest.batchId > _)).orElse(ready.headOption)
              selected match {
                case Some(batch) => lastDriven = Some(batch.manifest.batchId); proceed(batch, state)
                case None =>
                  if (pending.nonEmpty || state.batches.valuesIterator.exists(_.phase != "Blocked")) wake(settings.pollIntervalMillis)
                  Effect.none
              }
          }
        case Called(g, id, call, result) if g == generation && lease.isFresh && inFlight.get(id).exists(_._1 == call) =>
          inFlight(id)._2.cancel(); inFlight -= id
          state.batches.get(id) match {
            case Some(batch) if batch.phase != "Blocked" => result match {
              case Success(reply) => onResult(batch, reply)
              case Failure(_) => changed(batch.copy(nextDue = System.currentTimeMillis() + settings.pollIntervalMillis))()
            }
            case _ => wake(); Effect.none
          }
        case CallTimeout(g, id, call) if g == generation && lease.isFresh && inFlight.get(id).exists(_._1 == call) =>
          state.batches.get(id).map(batch => block(batch, "peer call timed out; reconcile the same resource/delivery identity"))
            .getOrElse(Effect.none)
        case _ => Effect.none
      }
    }
    val behavior = EventSourcedBehavior[Command, BatchEvent, BatchState](persistenceId, BatchState(), handler, BatchState.evolve)
      .receiveSignal { case (state, RecoveryCompleted) => if (state.jobId.nonEmpty && issue(state).isEmpty) wake() }
    Behaviors.intercept(() => new BehaviorSignalInterceptor[Command] {
      override def aroundSignal(context: TypedActorContext[Command], signal: Signal,
          target: BehaviorInterceptor.SignalTarget[Command]): Behavior[Command] = {
        if (signal == PostStop || signal == PreRestart) close()
        target(context, signal)
      }
    })(behavior)
  }
}
