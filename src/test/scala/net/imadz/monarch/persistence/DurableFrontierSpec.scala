package net.imadz.monarch.persistence

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import PersistentMonarch._

class DurableFrontierSpec extends AnyWordSpec with Matchers {
  private def bytes(value: String) = StateCodec.stringCodec.encode(value)
  private def start = evolve(DurableState(Idle, Vector.empty, None),
    RunStarted("frontier", Vector("a#0", "b#1", "c#2"), bytes("")))

  "Durable frontier" should {
    "advance resolved state without a clean completion" in {
      val a = evolve(start, StageDone("a#0", 1L, Map.empty, bytes("a")))
      val b = evolve(a, StageResolved("b#1", 2L, Some("REGROUP"), "REGROUP", "child refs", bytes("aR")))
      val c = evolve(b, StageDone("c#2", 3L, Map.empty, bytes("aRc")))
      recoveryProblem(c, start.plan) shouldBe None
      c.run.asInstanceOf[Executing].completedCursors shouldBe start.plan
      StateCodec.stringCodec.decode(c.run.asInstanceOf[Executing].lastSnapshot) shouldBe "aRc"
    }

    "reject a legacy resolved hole rather than use a later snapshot to rerun it" in {
      val a = evolve(start, StageDone("a#0", 1L, Map.empty, bytes("a")))
      val c = evolve(a, StageDone("c#2", 2L, Map.empty, bytes("aRc")))
      recoveryProblem(c, start.plan).get should include("non-contiguous")
    }

    "reject duplicate checkpoints" in {
      val a = evolve(start, StageDone("a#0", 1L, Map.empty, bytes("a")))
      val duplicate = evolve(a, StageDone("a#0", 2L, Map.empty, bytes("aa")))
      recoveryProblem(duplicate, start.plan) should not be empty
    }

    "allow old contiguous appended checkpoints and record the extended plan" in {
      val old = evolve(DurableState(Idle, Vector.empty, None), RunStarted("append", Vector("a#0"), bytes("")))
      val a = evolve(old, StageDone("a#0", 1L, Map.empty, bytes("a")))
      val b = evolve(a, StageDone("b#1", 2L, Map.empty, bytes("ab")))
      val current = Vector("a#0", "b#1", "c#2")
      recoveryProblem(b, current) shouldBe None
      val extended = evolve(b, RunPlanExtended(current))
      extended.plan shouldBe current
      recoveryProblem(extended, current) shouldBe None
      recoveryProblem(extended, Vector("a#0", "b#1")) should not be empty
    }

    "reject rewritten or shortened plans" in {
      recoveryProblem(start, Vector("a#0", "c#2", "b#1")) should not be empty
      recoveryProblem(start, Vector("a#0", "b#1")) should not be empty
      recoveryProblem(evolve(start, RunPlanExtended(Vector("x#0"))), start.plan) should not be empty
    }
  }
}
