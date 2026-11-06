package net.imadz.monarch.persistence

import akka.actor.Cancellable
import akka.actor.typed.{ActorRef, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import net.imadz.monarch.{RunLease, StaleRun}
import net.imadz.monarch.persistence.operation.OperationProtocol

import java.util.concurrent.Semaphore
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

final case class ManagedOperationSettings(
    queryTimeout: FiniteDuration = 5.seconds,
    pollInterval: FiniteDuration = 1.second,
    maxWaiters: Int = 1024
) {
  require(queryTimeout.length > 0 && pollInterval.length > 0, "managed query durations must be positive")
  require(maxWaiters > 0, "maxWaiters must be positive")
}

/** Share one instance across runs to bound local queries. Resolution must return the same
  * durable operation entity for a key; it must not create an anonymous new operation.
  */
final class ManagedOperations(
    val resolve: String => ActorRef[OperationProtocol.Command],
    val settings: ManagedOperationSettings = ManagedOperationSettings()
) {
  private val permits = new Semaphore(settings.maxWaiters)
  private[persistence] def acquire(): Boolean = permits.tryAcquire()
  private[persistence] def release(): Unit = permits.release()
  def activeWaiters: Int = settings.maxWaiters - permits.availablePermits()
}

/** Local, disposable scope. The Run's AwaitingRef and Operation journal are the durable facts.
  * R1 deliberately supports one operation per stage; repeated attachment to that request is safe.
  */
final class ManagedStageScope private[persistence] (
    val cursor: String,
    val stepId: String,
    lease: RunLease,
    runtime: ManagedOperations,
    bind: RunProtocol.AwaitingRef => Future[Unit],
    pause: String => Unit
)(implicit ec: ExecutionContext, scheduler: Scheduler) {
  val operations: ManagedStageScope = this
  private val lock = new Object
  private var request: Option[OperationProtocol.Request] = None
  private var result: Option[Promise[OperationProtocol.Terminal]] = None
  private var timer: Option[Cancellable] = None
  private var closed = false
  private var permit = false
  private var inFlight = false
  private var confirmed = false

  private def releaseIfIdle(): Unit = {
    if (permit && !inFlight && (closed || confirmed)) {
      permit = false
      runtime.release()
    }
  }

  private def refuse(reason: String): Future[OperationProtocol.Terminal] = {
    pause(reason) // Revokes the owner before the core can classify the failed Future.
    close()
    Future.failed(StaleRun)
  }

  def awaitTerminal(value: OperationProtocol.Request): Future[OperationProtocol.Terminal] = lock.synchronized {
    if (closed || !lease.isFresh) Future.failed(StaleRun)
    else if (value.stepId != stepId) refuse(s"operation stepId does not match $cursor")
    else request match {
      case Some(previous) if previous == value => result.get.future
      case Some(_) => refuse(s"stage $cursor attempted more than one operation identity/request")
      case None if !runtime.acquire() => refuse("managed operation waiting capacity unavailable; controlled recovery required")
      case None =>
        permit = true
        request = Some(value)
        val promise = Promise[OperationProtocol.Terminal]()
        result = Some(promise)
        val binding = try bind(RunProtocol.AwaitingRef(cursor, value.operationKey, value.fingerprint))
          catch { case NonFatal(ex) => Future.failed(ex) }
        binding.onComplete {
          case Success(_) => poll()
          case Failure(StaleRun) => close()
          case Failure(ex) => refuse(s"operation reference could not commit: ${ex.getClass.getSimpleName}"); ()
        }
        promise.future
    }
  }

  private def schedule(): Unit = lock.synchronized {
    if (!closed && !confirmed && lease.isFresh) {
      timer = Some(scheduler.scheduleOnce(runtime.settings.pollInterval, new Runnable {
        override def run(): Unit = poll()
      }))
    }
  }

  private def poll(): Unit = {
    val next = lock.synchronized {
      timer = None
      if (closed || confirmed || inFlight || !lease.isFresh) None
      else { inFlight = true; request }
    }
    next.foreach { value =>
      implicit val timeout: Timeout = Timeout(runtime.settings.queryTimeout)
      val queried = try runtime.resolve(value.operationKey).ask[OperationProtocol.Reply](reply => OperationProtocol.Ensure(value, reply))
        catch { case NonFatal(ex) => Future.failed(ex) }
      queried.onComplete { observed =>
        val active = lock.synchronized {
          inFlight = false
          releaseIfIdle()
          !closed && !confirmed && lease.isFresh
        }
        if (active) observed match {
          case Success(OperationProtocol.Current(view)) if view.operationKey == value.operationKey &&
              view.requestDigest.contains(value.fingerprint) => view.terminal match {
            case Some(terminal) if (view.phase == "Succeeded" || view.phase == "Failed") &&
                terminal.evidence.operationKey == value.operationKey &&
                terminal.evidence.requestDigest == value.fingerprint && terminal.evidence.ledgerEpoch == value.ledgerEpoch =>
              lock.synchronized {
                if (!closed && lease.isFresh) {
                  confirmed = true
                  releaseIfIdle()
                  result.foreach(_.trySuccess(terminal))
                }
              }
            case Some(_) => refuse("operation terminal evidence does not match the frozen request"); ()
            case None => schedule()
          }
          case Success(OperationProtocol.Rejected(_, reason)) => refuse(s"operation attachment rejected: $reason"); ()
          case Success(_) => refuse("operation view does not match the frozen request"); ()
          case Failure(_) => schedule() // A finite ask timeout is not a business outcome.
        }
      }
    }
  }

  private[persistence] def hasUnconfirmedOperation: Boolean = lock.synchronized(request.nonEmpty && !confirmed)

  private[persistence] def confirms(reference: RunProtocol.AwaitingRef): Boolean = lock.synchronized {
    confirmed && request.exists(value => reference.cursor == cursor &&
      reference.operationKey == value.operationKey && reference.requestDigest == value.fingerprint)
  }

  private[persistence] def close(): Unit = lock.synchronized {
    closed = true
    timer.foreach(_.cancel())
    timer = None
    result.foreach(_.tryFailure(StaleRun))
    // An outstanding finite ask still consumes its permit until it settles.
    releaseIfIdle()
  }
}
