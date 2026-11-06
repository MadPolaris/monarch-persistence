package net.imadz.monarch.persistence.resource

import net.imadz.monarch.persistence.resource.ResourceProtocol.{Grant, Proof}

/** Host-specific, pure, non-blocking validation of authoritative physical evidence.
  * The coordinator first checks operationKey == grant.ownerOperationKey. A timeout,
  * missing history, Idle or Unknown is not evidence of physical release. For a
  * transfer the proof must authorize ending the old owner's entire claim: the
  * requested subset stays occupied by the new owner, all other units are released.
  * There is no external I/O or validator callback during replay.
  */
trait ResourceEvidenceValidator {
  def validate(grant: Grant, proof: Proof): Either[String, Unit] =
    Left("authoritative resource evidence validator is not configured")
}

object ResourceEvidenceValidator {
  val reject: ResourceEvidenceValidator = new ResourceEvidenceValidator {}
}
