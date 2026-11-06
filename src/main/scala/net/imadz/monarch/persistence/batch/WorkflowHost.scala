package net.imadz.monarch.persistence.batch

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.persistence.typed.PersistenceId
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.{ByteString, Timeout}
import net.imadz.monarch.persistence._
import net.imadz.monarch.persistence.delivery.{DeliveryProtocol, DeliverySettings, PersistentInbox}
import net.imadz.monarch.persistence.operation.{ExternalOperationPort, OperationProtocol, OperationSettings, PersistentOperation}
import net.imadz.monarch.persistence.resource.{ResourceCoordinator, ResourceEvidenceValidator, ResourceProtocol, ResourceSettings}
import net.imadz.monarch.persistence.workflow._

import java.util.concurrent.{ConcurrentHashMap, Semaphore}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/** Host-owned business mapping. The runtime supplies all durable control protocols. */
trait WorkflowProgram {
  def definition: BatchDefinition
  def capacities: Map[String, Int]
  def resourceEvidence: ResourceEvidenceValidator
  def connector(area: String): ExternalOperationPort
  def operation(manifest: BatchManifest): OperationProtocol.Request
  def complete(manifest: BatchManifest, terminal: OperationProtocol.Terminal): Either[String, BatchCompletion]
  def verifyRoute(item: Item, area: String): Either[String, Unit] = Left("no verification route registered")
}

/** Local bootstrap for a bounded workflow. Stable namespaces make each cache disposable;
  * journals, not actor maps, own job/operation/child results. One instance owns a namespace.
  * Deploying sharded/multi-JVM hosts requires the same explicit single-writer routing contract.
  */
final class WorkflowHost(system: ActorSystem[_], namespace: String, program: WorkflowProgram,
    maxJobs: Int = 32, maxChildren: Int = 10000)(implicit ec: ExecutionContext) {
  require(namespace.nonEmpty && maxJobs > 0 && maxChildren > 0, "bounded workflow host identity required")
  private val lock = new Object
  private val jobs = new ConcurrentHashMap[String, ActorRef[BatchProtocol.Command]]()
  private val deliveries = new ConcurrentHashMap[String, ActorRef[DeliveryProtocol.Command]]()
  private val runs = new ConcurrentHashMap[String, ActorRef[PersistentMonarch.Command[ByteString]]]()
  private val operations = new ConcurrentHashMap[String, ActorRef[OperationProtocol.Command]]()
  private val requests = new ConcurrentHashMap[String, (OperationProtocol.Request, String)]()
  private val reconciliations = new Semaphore(maxJobs)
  private val operationSettings = OperationSettings.fromConfig(system.settings.config)
  private val deliverySettings = DeliverySettings.fromConfig(system.settings.config)
  private implicit val scheduler: akka.actor.typed.Scheduler = system.scheduler
  private implicit val queryTimeout: Timeout = Timeout(5.seconds)
  private def actorName(kind: String, id: String): String = kind + "-" + ProtocolDigest.sha256(ByteString(namespace + "|" + id)).take(32)
  private def pid(kind: String, id: String): PersistenceId = PersistenceId.ofUniqueId(namespace + "|" + kind + "|" + id)
  private def unique[K, V](cache: ConcurrentHashMap[K, V], key: K, limit: Int)(create: => V): V = lock.synchronized {
    Option(cache.get(key)).getOrElse {
      require(cache.size < limit, "workflow host active identity limit reached; use a new bounded host namespace")
      val created = create
      cache.put(key, created)
      created
    }
  }
  private val resource = system.systemActorOf(ResourceCoordinator.behavior(pid("resource", "domain"), program.capacities,
    program.resourceEvidence, ResourceSettings.fromConfig(system.settings.config)), actorName("resources", "domain"))
  private def operation(key: String, area: String): ActorRef[OperationProtocol.Command] = unique(operations, key, maxChildren) {
    require(program.definition.byId.contains(area), "unregistered operation area")
    system.systemActorOf(PersistentOperation.behavior(pid("operation", key), key, program.connector(area), operationSettings),
      actorName("operation", key))
  }
  private val managed = new ManagedOperations(key => {
    val (_, area) = Option(requests.get(key)).getOrElse(throw new IllegalArgumentException("unregistered operation identity"))
    operation(key, area)
  })
  private val codec = StateCodec[ByteString](_.toArray, bytes => ByteString.fromArray(bytes))
  private def runDefinition(area: String): RunDefinition[String] = RunDefinition(
    program.definition.id + "/" + area, program.definition.version, "batch-codec-1", Vector("execute"))
  private val runPort = RunSubmissionPort(submission => unique(runs, submission.runId, maxChildren) {
    val manifest = BatchCodec.manifestFromBytes(submission.initialState)
    require(manifest.runId == submission.runId, "submission manifest/run identity mismatch")
    val request = program.operation(manifest)
    require(request.operationKey == manifest.operationKey && request.stepId == "execute", "unstable workflow operation identity")
    val prior = requests.putIfAbsent(request.operationKey, request -> manifest.area)
    require(prior == null || prior == (request -> manifest.area), "workflow operation key conflict")
    val behavior: Behavior[PersistentMonarch.Command[ByteString]] = PersistentMonarch.managedBehavior(pid("run", submission.runId),
      runDefinition(manifest.area), (_: String, _: ByteString, scope: ManagedStageScope) =>
        scope.awaitTerminal(request).flatMap { terminal => program.complete(manifest, terminal) match {
          case Right(completion) => Future.successful(BatchCodec.completionToBytes(completion))
          case Left(reason) => Future.failed(new IllegalArgumentException(reason))
        }}, codec, managed)
    system.systemActorOf(behavior, actorName("run", submission.runId))
  }.narrow[RunProtocol.Request])
  private def delivery(id: String): ActorRef[DeliveryProtocol.Command] = unique(deliveries, id, maxChildren) {
    system.systemActorOf(PersistentInbox.behavior(pid("delivery", id), id, runPort, deliverySettings), actorName("delivery", id))
  }
  private val execution = new BatchExecutionPort {
    override def prepare(manifest: BatchManifest, grant: ResourceProtocol.Grant): Either[String, RunSubmission] = {
      val d = runDefinition(manifest.area)
      Right(RunSubmission(manifest.runId, d.id, d.version, d.codecVersion, BatchCodec.manifestToBytes(manifest)))
    }
    override def classify(manifest: BatchManifest, result: ByteString): Either[String, BatchCompletion] =
      Right(BatchCodec.completionFromBytes(result))
    override def verifyRoute(item: Item, area: String): Either[String, Unit] = program.verifyRoute(item, area)
  }
  def job(jobId: String): ActorRef[BatchProtocol.Command] = unique(jobs, jobId, maxJobs) {
    system.systemActorOf(BatchCoordinator.behavior(pid("job", jobId), jobId, program.definition, resource, delivery, execution),
      actorName("job", jobId))
  }
  /** One explicit, bounded operator action. Polling never calls this method. It
    * queries/reconciles only the original operation, then wakes the durable inbox
    * and job as needed. No result is invented and no identity or budget is reset.
    */
  def reconcile(jobId: String, batchId: String): Future[BatchProtocol.Reply] = {
    if (!reconciliations.tryAcquire()) Future.successful(BatchProtocol.Rejected("workflow reconciliation capacity reached"))
    else {
      val result = try {
        val coordinator = job(jobId)
        def current(): Future[BatchProtocol.Reply] = coordinator.ask[BatchProtocol.Reply](BatchProtocol.Get(_))
        current().flatMap {
          case BatchProtocol.Current(view) => view.batches.find(_.batchId == batchId) match {
            case None => Future.successful(BatchProtocol.Rejected("batch is not active in this committed job"))
            case Some(batch) if batch.deliveryId.isEmpty ||
                Set("ReleasingCarried", "Releasing", "Settling")(batch.phase) =>
              if (batch.phase == "Blocked") coordinator.ask[BatchProtocol.Reply](BatchProtocol.Reconcile(batchId, _))
              else current()
            case Some(batch) =>
              val key = batchId + "/execute"
              // The cache can be empty after a JVM restart while the job/inbox are
              // durably Blocked. Activate only this committed batch's original
              // operation; Get on an empty operation never submits it.
              val ref = operation(key, batch.area)
              val reconciledOperation = ref.ask[OperationProtocol.Reply](OperationProtocol.Get(key, _)).flatMap {
                case OperationProtocol.Current(v) if v.phase == "Blocked" =>
                  ref.ask[OperationProtocol.Reply](OperationProtocol.Reconcile(key, _))
                case other => Future.successful(other)
              }.map {
                case OperationProtocol.Rejected(_, reason) => Some(reason)
                case OperationProtocol.Current(v) if v.phase == "RecoveryBlocked" =>
                  Some(v.reason.getOrElse("operation recovery blocked"))
                case _ => None
              }
              reconciledOperation.flatMap {
                case Some(reason) => Future.successful(BatchProtocol.Rejected(reason))
                case None => current().flatMap {
                  case BatchProtocol.Current(v) if v.batches.exists(b => b.batchId == batchId && b.phase == "Blocked") =>
                    coordinator.ask[BatchProtocol.Reply](BatchProtocol.Reconcile(batchId, _))
                  case response @ BatchProtocol.Current(v) if !v.batches.exists(_.batchId == batchId) => Future.successful(response)
                  case response => Option(deliveries.get(batchId + "/delivery")) match {
                    case None => Future.successful(response) // Recovery may still be registering the committed child.
                    case Some(ref) => ref.ask[DeliveryProtocol.Reply](DeliveryProtocol.Get(batchId + "/delivery", _)).flatMap {
                      case DeliveryProtocol.Current(v) if v.phase == "Blocked" =>
                        ref.ask[DeliveryProtocol.Reply](DeliveryProtocol.Reconcile(batchId + "/delivery", _)).flatMap {
                          case DeliveryProtocol.Rejected(_, reason) => Future.successful(BatchProtocol.Rejected(reason))
                          case _ => current()
                        }
                      case _ => current()
                    }
                  }
                }
              }
          }
          case other => Future.successful(other)
        }
      } catch { case scala.util.control.NonFatal(ex) => Future.failed(ex) }
      result.andThen { case _ => reconciliations.release() }
    }
  }

  def resourceDomain: ActorRef[ResourceProtocol.Command] = resource
}
