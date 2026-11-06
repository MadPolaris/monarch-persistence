package net.imadz.monarch.persistence

import akka.actor.typed.{ActorRef, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.{ByteString, Timeout}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/** Frozen launch envelope. Durable callers persist this before sending it to a run host. */
final case class RunSubmission(runId: String, definitionId: String, definitionVersion: String,
    codecVersion: String, initialState: ByteString) {
  def fingerprint: String = ProtocolDigest.fields(Vector(ByteString("monarch-run-submission-v1"),
    ByteString(runId), ByteString(definitionId), ByteString(definitionVersion), ByteString(codecVersion), initialState))

  def validate(limits: ProtocolLimits): Option[String] =
    limits.text("runId", runId).orElse(limits.text("definitionId", definitionId))
      .orElse(limits.text("definitionVersion", definitionVersion)).orElse(limits.text("codecVersion", codecVersion))
      .orElse(limits.bytes("initialState", initialState, limits.maxStateBytes))
}

/** Calls the existing PersistentMonarch protocol, never a second stage executor.
  * Resolver ownership and persistence-id routing remain the host's responsibility.
  */
trait RunSubmissionPort {
  def ensure(submission: RunSubmission): Future[RunProtocol.Reply]
  def query(submission: RunSubmission): Future[RunProtocol.Reply]
}

object RunSubmissionPort {
  def apply(resolve: RunSubmission => ActorRef[RunProtocol.Request], queryTimeout: FiniteDuration = 5.seconds)
      (implicit ec: ExecutionContext, scheduler: Scheduler): RunSubmissionPort = {
    require(queryTimeout.length > 0, "run query timeout must be positive")
    new RunSubmissionPort {
      private implicit val timeout: Timeout = Timeout(queryTimeout)
      override def ensure(submission: RunSubmission): Future[RunProtocol.Reply] =
        resolve(submission).ask(replyTo => RunProtocol.StartOrAttach(submission.runId, submission.definitionId,
          submission.definitionVersion, submission.codecVersion, submission.initialState, replyTo))
      override def query(submission: RunSubmission): Future[RunProtocol.Reply] =
        resolve(submission).ask(replyTo => RunProtocol.GetRun(replyTo))
    }
  }
}
