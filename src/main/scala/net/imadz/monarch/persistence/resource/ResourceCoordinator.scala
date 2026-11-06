package net.imadz.monarch.persistence.resource

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.{PersistenceId, RecoveryCompleted}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import net.imadz.monarch.persistence.resource.ResourceProtocol._

import scala.util.control.NonFatal

/** Durable all-or-none admission for one statically defined, exclusively controlled
  * resource domain. The host supplies a reliable journal and a single writer; this
  * is neither a cross-domain transaction nor protection against split brain.
  *
  * Admission uses the oldest currently fitting bundle. A blocked incoming FOUP must
  * not prevent an existing occupant's continuation from releasing its destination.
  * Queued clients query; no actor refs or outstanding asks are retained. No clocks or timeouts free units.
  * All IDs and terminal receipts remain until this bounded domain is retired.
  */
object ResourceCoordinator {
  def apply(persistenceId: PersistenceId, capacities: Map[String, Int],
      validator: ResourceEvidenceValidator = ResourceEvidenceValidator.reject,
      settings: ResourceSettings = ResourceSettings()): Behavior[Command] =
    behavior(persistenceId, capacities, validator, settings)

  def behavior(persistenceId: PersistenceId, capacities: Map[String, Int],
      validator: ResourceEvidenceValidator = ResourceEvidenceValidator.reject,
      settings: ResourceSettings = ResourceSettings()): Behavior[Command] = Behaviors.setup { ctx =>
    require(persistenceId != null && validator != null && settings != null, "resource configuration is required")
    val expected = ResourceDefinition(persistenceId.id, capacities, settings)
    ResourceValidation.requireValid(ResourceValidation.definition(expected))
    ResourceValidation.requireValid(ResourceValidation.state(ResourceState(definition = Some(expected))))
    val limits = settings.limits
    var localProblem: Option[String] = None

    def problem(state: ResourceState): Option[String] = localProblem.orElse(state.invalid).orElse {
      state.definition.filter(_ != expected).map(_ => "frozen resource capacities/configuration differ from this host")
    }
    def safeId(id: String): String = if (limits.text("reservation id", id).isEmpty) id else "invalid-reservation"
    def reject(id: String, to: ActorRef[Reply], why: String): Effect[ResourceEvent, ResourceState] = {
      if (to != null) to ! Rejected(safeId(id), limits.diagnostic(why))
      Effect.none
    }
    def reply(to: ActorRef[Reply], value: Reply): Effect[ResourceEvent, ResourceState] = {
      if (to != null) to ! value
      Effect.none
    }
    def current(state: ResourceState, id: String): Reply = state.records.get(id) match {
      case None => Unknown(id, "reservation is not recorded")
      case Some(r) if !r.granted => Queued(id)
      case Some(r) => r.closure match {
        case None => Granted(ResourceIdentity.grant(state.definition.get, r.request))
        case Some(c) if c.targetReservationId.isEmpty => Released(id)
        case Some(_) => Rejected(id, "reservation was transferred; query the destination claim")
      }
    }
    def commit(state: ResourceState, event: ResourceEvent, id: String, to: Option[ActorRef[Reply]],
        response: ResourceState => Reply, drive: Boolean): Effect[ResourceEvent, ResourceState] = {
      val next = ResourceState.evolve(state, event)
      val error = next.invalid.orElse {
        if (ResourceProtoConverters.eventToProto(event).serializedSize > limits.maxEnvelopeBytes)
          Some("resource event exceeds configured envelope limit") else None
      }
      error match {
        case Some(why) => to match {
          case Some(ref) => reject(id, ref, why)
          case None => localProblem = Some(why); Effect.none
        }
        case None => Effect.persist(event).thenRun { committed: ResourceState =>
          to.foreach(ref => if (ref != null) ref ! response(committed))
          if (drive) ctx.self ! Drive
        }
      }
    }
    def validateProof(grant: Grant, proof: Proof): Option[String] =
      if (proof.operationKey != grant.ownerOperationKey) Some("proof operation key differs from grant owner")
      else try validator.validate(grant, proof) match {
        case Right(_) => None
        case Left(reason) => Some(limits.diagnostic(Option(reason).filter(_.nonEmpty).getOrElse("resource evidence rejected")))
        case null => Some("resource evidence validator returned no decision")
      } catch { case NonFatal(_) => Some("resource evidence validator unavailable; occupancy retained") }

    def handle(state: ResourceState, command: Command): Effect[ResourceEvent, ResourceState] = {
      val blocked = problem(state)
      if (blocked.nonEmpty) command match {
        case Query(id, to) => reply(to, Unknown(safeId(id), limits.diagnostic("resource domain blocked: " + blocked.get)))
        case Reserve(r, to) => reject(Option(r).map(_.reservationId).orNull, to, blocked.get)
        case Release(id, _, _, to) => reject(id, to, blocked.get)
        case Transfer(id, _, _, _, _, _, to) => reject(id, to, blocked.get)
        case Drive => Effect.none
      }
      else if (state.definition.isEmpty) command match {
        // RecoveryCompleted schedules this before any admission can run. Commands
        // arriving first are stashed until the initial definition commits.
        case Drive => Effect.persist[ResourceEvent, ResourceState](
          ResourceEvent.Defined(expected, expected.fingerprint)).thenUnstashAll()
        case _ => Effect.stash()
      }
      else command match {
        case Drive => state.nextGrant match {
          case Some(id) =>
            val grant = ResourceIdentity.grant(expected, state.records(id).request)
            commit(state, ResourceEvent.GrantRecorded(grant), id, None, _ => Granted(grant), drive = true)
          case _ => Effect.none
        }
        case c @ Query(id, to) => ResourceValidation.wire(c, limits) match {
          case Some(why) => reject(id, to, why)
          case None => reply(to, current(state, id)) // Deliberately no Drive, persist or host I/O.
        }
        case c @ Reserve(request, to) =>
          val id = Option(request).map(_.reservationId).orNull
          ResourceValidation.wire(c, limits).orElse(ResourceValidation.demand(request, expected)) match {
            case Some(why) => reject(id, to, why)
            case None => state.records.get(id) match {
              case Some(r) if r.request != request => reject(id, to, "reservation id conflicts with its immutable request")
              case Some(_) => reply(to, current(state, id))
              case None if state.records.size >= settings.maxReservations =>
                reject(id, to, "resource lifetime reservation limit reached; IDs are not evicted")
              case None if state.nextGrant.isEmpty && state.fits(request.units) =>
                val grant = ResourceIdentity.grant(expected, request)
                commit(state, ResourceEvent.GrantRecorded(grant), id, Some(to), _ => Granted(grant), drive = false)
              case None if state.queue.size >= settings.maxQueued => reject(id, to, "resource FIFO queue is full")
              case None => commit(state, ResourceEvent.Enqueued(request), id, Some(to), _ => Queued(id), drive = true)
            }
          }
        case c @ Release(id, token, proof, to) => ResourceValidation.wire(c, limits) match {
          case Some(why) => reject(id, to, why)
          case None => state.records.get(id) match {
            case None => reply(to, Unknown(id, "reservation is not recorded; no occupancy was released"))
            case Some(r) if !r.granted => reject(id, to, "queued reservation is not a grant")
            case Some(r) =>
              val grant = ResourceIdentity.grant(expected, r.request)
              if (token != grant.token) reject(id, to, "grant token mismatch")
              else if (proof.operationKey != grant.ownerOperationKey) reject(id, to, "proof operation key differs from grant owner")
              else r.closure match {
                case Some(c) if c.targetReservationId.nonEmpty => reject(id, to, "transferred claim cannot be released again")
                case Some(c) if c.fingerprint == ResourceIdentity.release(id, token, proof) => reply(to, Released(id))
                case Some(_) => reject(id, to, "conflicting replay of a committed release")
                case None => validateProof(grant, proof) match {
                  case Some(why) => reject(id, to, why)
                  case None => commit(state, ResourceEvent.ReleaseRecorded(id, token, proof), id, Some(to),
                    _ => Released(id), drive = true)
                }
              }
          }
        }
        case c @ Transfer(id, token, targetId, owner, units, proof, to) =>
          val request = Request(targetId, owner, units)
          ResourceValidation.wire(c, limits).orElse(ResourceValidation.demand(request, expected)) match {
            case Some(why) => reject(id, to, why)
            case None => state.records.get(id) match {
              case None => reply(to, Unknown(id, "source reservation is not recorded"))
              case Some(r) if !r.granted => reject(id, to, "queued reservation is not a grant")
              case Some(r) =>
                val grant = ResourceIdentity.grant(expected, r.request)
                if (token != grant.token) reject(id, to, "grant token mismatch")
                else if (proof.operationKey != grant.ownerOperationKey) reject(id, to, "proof operation key differs from grant owner")
                else r.closure match {
                  case Some(c) if c.targetReservationId.contains(targetId) &&
                      c.fingerprint == ResourceIdentity.transfer(id, token, request, proof) =>
                    // Return the immutable receipt even if the destination has
                    // subsequently completed. Query(targetId) is its current view.
                    reply(to, Granted(ResourceIdentity.grant(expected, state.records(targetId).request)))
                  case Some(_) => reject(id, to, "source already closed by a different release/transfer")
                  case None if state.records.contains(targetId) => reject(id, to, "transfer destination id already exists")
                  case None if state.records.size >= settings.maxReservations => reject(id, to, "transfer requires a new lifetime record")
                  case None if !units.forall { case (key, n) => r.request.units.get(key).exists(_ >= n) } =>
                    reject(id, to, "transfer must retain a subset of the entire source grant")
                  case None => validateProof(grant, proof) match {
                    case Some(why) => reject(id, to, why)
                    case None =>
                      val target = ResourceIdentity.grant(expected, request)
                      commit(state, ResourceEvent.TransferRecorded(id, token, target, proof), id, Some(to),
                        _ => Granted(target), drive = true)
                  }
                }
            }
          }
      }
    }

    EventSourcedBehavior[Command, ResourceEvent, ResourceState](persistenceId, ResourceState(), handle,
      ResourceState.evolve).receiveSignal {
      case (_, RecoveryCompleted) => ctx.self ! Drive
    }.snapshotWhen((_, _, sequenceNr) => sequenceNr % 100L == 0L)
  }
}
