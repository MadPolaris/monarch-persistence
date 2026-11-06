package net.imadz.monarch.persistence.resource

import akka.actor.typed.{ActorRef, ActorSystem, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import net.imadz.monarch.persistence.resource.ResourceProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** Each call is one finite ask. Unknown means unavailable, never cancellation or
  * physical release. A durable caller retries the same ID and immutable command.
  * In particular, neither this port nor Query turns Queued into launch permission.
  */
final class ResourceAdmissionPort(
    coordinator: ActorRef[Command],
    askTimeout: FiniteDuration = 3.seconds
)(implicit scheduler: Scheduler, ec: ExecutionContext) {
  require(coordinator != null, "resource coordinator is required")
  require(askTimeout > Duration.Zero && askTimeout <= 7.days, "resource ask timeout must be in (0, 7 days]")
  private implicit val timeout: Timeout = Timeout(askTimeout)
  private def ask(id: String)(message: ActorRef[Reply] => Command): Future[Reply] = {
    def unavailable: Reply = Unknown(
      if (ResourceValidation.Ceilings.text("reservation id", id).isEmpty) id else "invalid-reservation",
      "resource coordinator unavailable; occupancy has not been released")
    try coordinator.ask[Reply](message).recover { case NonFatal(_) => unavailable }
    catch { case NonFatal(_) => Future.successful(unavailable) }
  }
  def reserve(request: Request): Future[Reply] = ask(request.reservationId)(Reserve(request, _))
  def query(reservationId: String): Future[Reply] = ask(reservationId)(Query(reservationId, _))
  def release(grant: Grant, proof: Proof): Future[Reply] =
    ask(grant.reservationId)(Release(grant.reservationId, grant.token, proof, _))
  def transfer(grant: Grant, toReservationId: String, toOwnerOperationKey: String,
      units: Map[String, Int], proof: Proof): Future[Reply] = ask(grant.reservationId)(Transfer(
    grant.reservationId, grant.token, toReservationId, toOwnerOperationKey, units, proof, _))
}

object ResourceAdmissionPort {
  def apply(coordinator: ActorRef[Command], askTimeout: FiniteDuration = 3.seconds)
      (implicit system: ActorSystem[_]): ResourceAdmissionPort =
    new ResourceAdmissionPort(coordinator, askTimeout)(system.scheduler, system.executionContext)
}
