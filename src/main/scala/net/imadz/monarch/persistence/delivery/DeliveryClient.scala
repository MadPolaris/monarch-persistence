package net.imadz.monarch.persistence.delivery

import akka.actor.typed.{ActorRef, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import net.imadz.monarch.persistence.delivery.DeliveryProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** Finite single asks, not a long-lived await or an in-memory retry queue.
  * A durable caller repeats the same request after a lost ACK, then queries
  * until Completed when its settlement contract requires child completion.
  */
final class DeliveryClient(resolve: String => ActorRef[Command], timeout: FiniteDuration = 5.seconds)
    (implicit ec: ExecutionContext, scheduler: Scheduler) {
  require(resolve != null, "delivery actor resolver is required")
  require(timeout.length > 0L, "delivery ask timeout must be positive")
  private implicit val askTimeout: Timeout = Timeout(timeout)

  private def ask(id: String)(message: ActorRef[Reply] => Command): Future[Reply] =
    try resolve(id).ask(message)
    catch { case NonFatal(ex) => Future.failed(ex) }

  def deliver(req: Request): Future[Reply] =
    if (req == null) Future.failed(new IllegalArgumentException("delivery request is required"))
    else ask(req.deliveryId)(Deliver(req, _))

  def get(id: String): Future[Reply] = ask(id)(Get(id, _))
  def reconcile(id: String): Future[Reply] = ask(id)(Reconcile(id, _))
}
