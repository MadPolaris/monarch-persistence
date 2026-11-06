package net.imadz.monarch.persistence

import akka.util.ByteString
import net.imadz.monarch.persistence.batch._
import net.imadz.monarch.persistence.operation.{ExternalOperationPort, OperationProtocol}
import net.imadz.monarch.persistence.resource.{ResourceEvidenceValidator, ResourceProtocol}
import net.imadz.monarch.persistence.workflow._

import scala.concurrent.{ExecutionContext, Future, blocking}

/** Bank/Fab adapter policies for the same bounded public workflow runtime. The external
  * service is an independent process; no observer mutates accounts or physical outcomes.
  */
final class CompleteWorkflowProgram(kind: String, externalPort: Int)(implicit io: ExecutionContext) extends WorkflowProgram {
  require(kind == "bank" || kind == "fab")
  private val settings = BatchSettings(maxAcceptedItems = 100, maxActiveItems = 20, maxActiveBytes = 131072,
    pollIntervalMillis = 100, callTimeoutMillis = 3000, maxBatchCalls = 1000)
  override val definition: BatchDefinition = if (kind == "bank") BatchDefinition("bank-complete", "1", Vector(
    AreaDefinition("recharge", "1", BatchPolicy.countOrWait(3, 300), Map("ftp" -> 1), parallelism = 2, maxExecutions = 2),
    AreaDefinition("purchase", "1", BatchPolicy.single, Map("purchase-system" -> 1), parallelism = 2, maxExecutions = 1)), settings)
  else BatchDefinition("fab-complete", "1", Vector(
    AreaDefinition("transport-in", "1", BatchPolicy.single, Map("amhs" -> 1, "litho-port" -> 1)),
    AreaDefinition("litho", "1", BatchPolicy.single, Map("litho" -> 1)),
    AreaDefinition("transport-out", "1", BatchPolicy.single, Map("amhs" -> 1, "metrology-port" -> 1)),
    AreaDefinition("measure", "1", BatchPolicy.single, Map("metrology" -> 1), maxExecutions = 2),
    AreaDefinition("rework", "1", BatchPolicy.single, Map("rework-device" -> 1), maxExecutions = 1),
    AreaDefinition("unload", "1", BatchPolicy.single, Map("amhs" -> 1), maxExecutions = 1)), settings)
  override val capacities: Map[String, Int] = if (kind == "bank") Map("ftp" -> 1, "purchase-system" -> 1)
    else Map("amhs" -> 1, "litho-port" -> 1, "litho" -> 1, "metrology-port" -> 1, "metrology" -> 1, "rework-device" -> 1)

  private def encodeId(id: String): String = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(id.getBytes("UTF-8"))
  private def decodeId(id: String): String = new String(java.util.Base64.getUrlDecoder.decode(id), "UTF-8")
  private def businessPayload(m: BatchManifest): String = "WF2:" + m.area + ":" +
    m.items.map(i => encodeId(i.itemId) + "~" + i.execution).mkString(",")
  private def outcomes(result: ByteString, m: BatchManifest): Either[String, Map[String, String]] = try {
    val sections = result.utf8String.split(":", 3)
    if (sections.length != 3 || sections(0) != "WR2" || sections(1) != m.area) Left("wrong response area")
    else {
      val rows = sections(2).split(",").toVector.map { r => val v = r.split("~", 2); decodeId(v(0)) -> v(1) }
      if (rows.map(_._1).distinct.size != rows.size || rows.map(_._1).toSet != m.items.map(_.itemId).toSet ||
          rows.exists(r => !Set("SUCCESS", "RETRY", "REWORK")(r._2))) Left("incomplete or conflicting per-item response")
      else Right(rows.toMap)
    }
  } catch { case _: IllegalArgumentException | _: IndexOutOfBoundsException => Left("malformed response") }
  override def operation(m: BatchManifest): OperationProtocol.Request = OperationProtocol.Request(m.operationKey,
    m.items.map(i => s"${i.itemId}:${m.area}:${i.execution}").mkString("effect:", ",", ""), "execute", kind, "complete-1", "ledger-1",
    BatchCodec.manifestToBytes(m), Some(m.reservationId))

  override def connector(area: String): ExternalOperationPort = new ExternalOperationPort {
    import OperationProtocol._
    override val connectorId = kind
    override val connectorVersion = "complete-1"
    private def observe(req: Request, response: Vector[String], query: Boolean): Observation = {
      val suffix = if (query) 2 else 0
      def proof(result: String) = Evidence(if (query) response(response.size - 2) else req.operationKey,
        if (query) response.last else req.fingerprint, response(2), Some(response(1)), ByteString(result))
      response.headOption match {
        case Some("RUNNING") if response.size == 3 + suffix => if (query) Running(proof("running")) else Accepted(proof("running"))
        case Some("SUCCEEDED") if response.size == 4 + suffix => Confirmed(Succeeded(ByteString(response(3)), proof(response(3))))
        case Some("NOT_FOUND") => NotFound("authoritative task history unavailable")
        case _ => Unknown("external response rejected or unrecognized")
      }
    }
    override def submit(req: Request): Future[Observation] = Future(blocking {
      observe(req, ExternalTaskServiceFixture.call(externalPort, "SUBMIT", Vector(kind, req.operationKey, req.fingerprint,
        req.ledgerEpoch, req.effectKey, businessPayload(BatchCodec.manifestFromBytes(req.payload)))), query = false)
    })
    override def query(req: Request, previous: Option[Evidence]): Future[Observation] = Future(blocking {
      observe(req, ExternalTaskServiceFixture.call(externalPort, "QUERY", Vector(req.operationKey)), query = true)
    })
    override def validateEvidence(req: Request, observation: WithEvidence): Either[String, Unit] = {
      val m = BatchCodec.manifestFromBytes(req.payload)
      val valid = m.area == area && m.operationKey == req.operationKey && observation.evidence.taskId.exists(_.startsWith("task-")) &&
        (observation match {
          case _: Accepted | _: Running => observation.evidence.payload == ByteString("running")
          case Confirmed(Succeeded(result, proof)) => outcomes(result, m).isRight && proof.payload == result
          case _ => false
        })
      if (valid) Right(()) else Left("bank/Fab receipt does not match frozen members, area or operation")
    }
  }

  // Fixture receipts are not cryptographic authorization. Bind the recorded external
  // result to the exact job/operation and, for persistent occupancy, the actual FOUP
  // and move that can free its old port. A different device's success is insufficient.
  override val resourceEvidence: ResourceEvidenceValidator = new ResourceEvidenceValidator {
    override def validate(grant: ResourceProtocol.Grant, proof: ResourceProtocol.Proof): Either[String, Unit] = try {
      val lines = proof.evidence.utf8String.split("\n", -1)
      val parts = if (lines.length == 2) lines(1).split(":", -1) else Array.empty[String]
      require(proof.operationKey == grant.ownerOperationKey && lines.length == 2 && parts.length == 3 && parts(0) == "WR2")
      val executing = lines(0).split("/", -1)
      require(executing.length == 3 && executing(1).matches("batch-[0-9]+") && executing(2) == "execute")
      val rows = parts(2).split(",", -1).toVector.map { row =>
        val fields = row.split("~", -1)
        require(fields.length == 2 && Set("SUCCESS", "RETRY", "REWORK")(fields(1)))
        val id = decodeId(fields(0))
        require(encodeId(id) == fields(0) && id.startsWith(executing(0) + "-item-"))
        id -> fields(1)
      }
      require(rows.nonEmpty && rows.map(_._1).distinct.size == rows.size)
      val area = definition.byId(parts(1))
      if (grant.ownerOperationKey == lines(0)) {
        require(grant.reservationId == executing.take(2).mkString("/") + "/resources" && grant.units == area.resources)
      } else {
        val owner = grant.ownerOperationKey.split("/", -1)
        require(kind == "fab" && owner.length == 3 && owner(0) == executing(0) && owner(1) == "foup" &&
          rows == Vector(owner(2) -> "SUCCESS"))
        val metrology = parts(1) == "unload"
        require((parts(1) == "transport-out" || metrology) &&
          grant.units == Map((if (metrology) "metrology-port" else "litho-port") -> 1) &&
          grant.reservationId == owner(0) + (if (metrology) "/occupancy/metrology/" else "/occupancy/") + owner(2))
      }
      Right(())
    } catch { case scala.util.control.NonFatal(_) => Left("external receipt cannot release this operation/FOUP occupancy") }
  }

  override def complete(m: BatchManifest, terminal: OperationProtocol.Terminal): Either[String, BatchCompletion] = terminal match {
    case OperationProtocol.Succeeded(result, _) if outcomes(result, m).isRight =>
      val confirmed = outcomes(result, m).toOption.get
      val decisions = m.items.map { item =>
        val disposition: ItemDisposition = if (kind == "bank") m.area match {
          case "recharge" if confirmed(item.itemId) == "RETRY" => ItemDisposition.Retry(200)
          case "recharge" if confirmed(item.itemId) == "SUCCESS" => ItemDisposition.Route("purchase", "fund-1")
          case "purchase" if confirmed(item.itemId) == "SUCCESS" => ItemDisposition.Completed(Vector[Byte](1))
          case _ => ItemDisposition.Hold("unrecognized bank disposition")
        } else m.area match {
          case "transport-in" if confirmed(item.itemId) == "SUCCESS" => ItemDisposition.Route("litho", item.compatibilityKey)
          case "litho" if confirmed(item.itemId) == "SUCCESS" => ItemDisposition.Route("transport-out", item.compatibilityKey)
          case "transport-out" if confirmed(item.itemId) == "SUCCESS" => ItemDisposition.Route("measure", item.compatibilityKey)
          case "measure" if confirmed(item.itemId) == "REWORK" => ItemDisposition.Route("rework", item.compatibilityKey)
          case "rework" if confirmed(item.itemId) == "SUCCESS" => ItemDisposition.Route("measure", item.compatibilityKey)
          case "measure" if confirmed(item.itemId) == "SUCCESS" => ItemDisposition.Route("unload", item.compatibilityKey)
          case "unload" if confirmed(item.itemId) == "SUCCESS" => ItemDisposition.Completed(Vector[Byte](1))
          case _ => ItemDisposition.Hold("unrecognized Fab disposition")
        }
        ItemDecision(item.itemId, disposition)
      }
      val resourceProof = (ByteString(m.operationKey + "\n") ++ result).toVector
      val resource: ResourceDisposition = if (kind == "fab" && m.area == "transport-in")
        ResourceDisposition.Carry(m.jobId + "/occupancy/" + m.items.head.itemId, m.jobId + "/foup/" + m.items.head.itemId,
          Map("litho-port" -> 1), resourceProof)
      else if (kind == "fab" && m.area == "transport-out")
        ResourceDisposition.Carry(m.jobId + "/occupancy/metrology/" + m.items.head.itemId, m.jobId + "/foup/" + m.items.head.itemId,
          Map("metrology-port" -> 1), resourceProof)
      else ResourceDisposition.Release(resourceProof)
      Right(BatchCompletion(decisions, resource, retainCarried = kind == "fab" && Set("litho", "measure", "rework")(m.area)))
    case _ => Left("external operation did not provide confirmed completion evidence")
  }
}
