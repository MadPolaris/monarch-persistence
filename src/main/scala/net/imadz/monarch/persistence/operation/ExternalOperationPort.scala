package net.imadz.monarch.persistence.operation

import akka.util.ByteString
import net.imadz.monarch.persistence.operation.OperationProtocol._

import scala.concurrent.Future

/** A connector must query by the original Request, even if no task receipt was
  * received. ledgerEpoch identifies the external DURABLE acceptance ledger, not
  * its current process. Losing that ledger is not evidence that work never ran.
  *
  * Validators must be pure, non-blocking and check connector-specific proof,
  * including result/status binding. Generic key/digest/epoch/task checks are
  * performed first by the runtime. Both proof hooks deliberately fail closed.
  * submit/query should return promptly; their invocation also runs on the supplied
  * ExecutionContext, which must be appropriate for the connector's I/O.
  */
trait ExternalOperationPort {
  def connectorId: String
  def connectorVersion: String
  def submit(request: Request): Future[Observation]
  def query(request: Request, lastEvidence: Option[Evidence]): Future[Observation]

  def validateEvidence(request: Request, observation: WithEvidence): Either[String, Unit] =
    Left("connector evidence validation is not configured")

  /** Invoked only for a query's SafeToSubmit, after generic and connector proof
    * validation. Right(()) attests same-key idempotence OR authoritative proof
    * that the previous attempt cannot subsequently take effect. NotFound, Idle,
    * process restarts and elapsed time alone cannot authorize this.
    */
  def validateSafeToSubmit(request: Request, proof: Evidence,
      lastEvidence: Option[Evidence]): Either[String, Unit] =
    Left("connector has not authorized safe resubmission")
}

object ExternalOperationPort {
  /** Optional typed payload bridge, with no second runtime or unsafe Any casts.
    * The immutable envelope (including its original bytes/digest) is still passed
    * to every call. Encode business inputs before constructing Request; decode
    * result bytes after a committed Terminal is obtained.
    */
  trait PayloadCodec[A] {
    def encode(value: A): ByteString
    def decode(bytes: ByteString): Either[String, A]
  }

  abstract class Typed[A](codec: PayloadCodec[A]) extends ExternalOperationPort {
    protected def submitTyped(request: Request, payload: A): Future[Observation]
    protected def queryTyped(request: Request, payload: A,
        lastEvidence: Option[Evidence]): Future[Observation]

    final override def submit(request: Request): Future[Observation] =
      codec.decode(request.payload).fold(
        problem => Future.failed(new IllegalArgumentException(problem)),
        payload => submitTyped(request, payload))

    final override def query(request: Request, lastEvidence: Option[Evidence]): Future[Observation] =
      codec.decode(request.payload).fold(
        problem => Future.failed(new IllegalArgumentException(problem)),
        payload => queryTyped(request, payload, lastEvidence))
  }
}
