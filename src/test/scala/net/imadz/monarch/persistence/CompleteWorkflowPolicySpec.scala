package net.imadz.monarch.persistence

import akka.util.ByteString
import net.imadz.monarch.persistence.resource.ResourceProtocol
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

class CompleteWorkflowPolicySpec extends AnyWordSpec with Matchers {
  private implicit val ec: ExecutionContext = ExecutionContext.global
  private def receipt(area: String, item: String): String = "WR2:" + area + ":" +
    java.util.Base64.getUrlEncoder.withoutPadding().encodeToString(item.getBytes("UTF-8")) + "~SUCCESS"

  "The independent workflow fixture's resource policy" should {
    "free a carried FOUP only on that FOUP's correct outbound move" in {
      val program = new CompleteWorkflowProgram("fab", 1)
      val grant = ResourceProtocol.Grant("job/occupancy/job-item-0", "job/foup/job-item-0", "token", Map("litho-port" -> 1))
      def proof(area: String, item: String, operation: String = "job/batch-2/execute") = ResourceProtocol.Proof(
        grant.ownerOperationKey, ByteString(operation + "\n" + receipt(area, item)))
      program.resourceEvidence.validate(grant, proof("transport-out", "job-item-0")) shouldBe Right(())
      program.resourceEvidence.validate(grant, proof("litho", "job-item-0")).isLeft shouldBe true
      program.resourceEvidence.validate(grant, proof("transport-out", "job-item-1")).isLeft shouldBe true
      program.resourceEvidence.validate(grant, proof("transport-out", "job-item-0", "other/batch-2/execute")).isLeft shouldBe true
      program.resourceEvidence.validate(grant, proof("unload", "job-item-0")).isLeft shouldBe true
      val metrology = grant.copy(reservationId = "job/occupancy/metrology/job-item-0", units = Map("metrology-port" -> 1))
      program.resourceEvidence.validate(metrology, proof("unload", "job-item-0")) shouldBe Right(())
      program.resourceEvidence.validate(metrology, proof("measure", "job-item-0")).isLeft shouldBe true
    }

    "reject a different operation's successful result for execution capacity" in {
      val program = new CompleteWorkflowProgram("bank", 1)
      val grant = ResourceProtocol.Grant("job/batch-0/resources", "job/batch-0/execute", "token", Map("ftp" -> 1))
      val valid = ResourceProtocol.Proof(grant.ownerOperationKey,
        ByteString(grant.ownerOperationKey + "\n" + receipt("recharge", "job-item-0")))
      program.resourceEvidence.validate(grant, valid) shouldBe Right(())
      program.resourceEvidence.validate(grant, valid.copy(evidence =
        ByteString("job/batch-1/execute\n" + receipt("recharge", "job-item-0")))).isLeft shouldBe true
      program.resourceEvidence.validate(grant, valid.copy(evidence =
        ByteString(grant.ownerOperationKey + "\n" + receipt("purchase", "job-item-0")))).isLeft shouldBe true
    }
  }
}
