package learning

import opt.{QueryInstruction, RelationStub}

/** This class needs to sample from a space of possible outcomes.
  * Set(Transformations) - need a way to compute that's not tied the instruction implementation
  * Rejection and MC -
  */
class Sampler {
  
}

/** Class that checks whether a sampled plan returns the same result as the original instruction. */
class PlanValidator(val proposedPlan : QueryInstruction, val plan : Option[QueryInstruction] = None, val expectedResult : Option[RelationStub] = None) {
  def validate() : Boolean = {
    val expected  = if (plan.isDefined) {plan.get.execute} else {expectedResult.get}
    expected.equals(proposedPlan.execute)
  }
}

/** Rejection sampling is MC sampling with all the actions. If possible actions is implemented by the user, then use MC, otherwise, use Rejection sampling. */