package learning

import opt.{QueryInstruction, RelationStub, Transformation}

import dml.{IdentityTransform, OtherTransform}

import scala.collection.mutable
import scala.util.Random

/** This class needs to sample from a space of possible outcomes.
  * We allow for two forms of sampling - the first is Monte-Carlo sampling, where the implementer of the instruction
  * defines a function getAllowedTransformations on a given plan. This is significant overhead to the implementer, but
  * more efficient to sample fast. Rejection sampling is MC sampling with all the actions as allowed transformations,
  * and the sampler will default to this if the instruction does not implement the allowed transformations API.
  */
class Sampler(var initialPlan : QueryInstruction, var sampleDepth : Int, var maxDepth : Option[Int] = None) {

  val isMC : Boolean = initialPlan.allowedTransformations.isDefined
  val allTransformations : Vector[Transformation] = mutable.Set[Transformation](new IdentityTransform, new OtherTransform).toVector
  val transformationMaxAttempts : Int = maxDepth.getOrElse(1000)

  @throws(classOf[Exception])
  private def randomSampleMC() : Array[Transformation] = {
    val rnd = new Random()
    val transformationList = new Array[Transformation](sampleDepth)
    var planCopy = initialPlan.clone()
    for (i <- transformationList.indices) {
      val tSet = initialPlan.getAllowedTransformations(planCopy)
      val randomTransform = tSet.toVector(rnd.nextInt(tSet.size))
      transformationList.update(i, randomTransform)
      planCopy = randomTransform.transform(planCopy)
    }
    if (!new PlanValidator(planCopy, Some(initialPlan)).validate()) {
      throw new Exception("MC sampler has generated an invalid plan.")
    }
    transformationList
  }

  @throws(classOf[Exception])
  private def randomSampleRejection() : Array[Transformation] = {
    var i = 0
    while (i < transformationMaxAttempts) {
      /** Sample a transformation order */
      val sampledTransform = Array.fill(sampleDepth){allTransformations(scala.util.Random.nextInt(allTransformations.size))}
      var planCopy = initialPlan.clone()
      for (x <- sampledTransform) {
        planCopy = x.transform(planCopy)
      }
      if (new PlanValidator(planCopy, Some(initialPlan)).validate()) {
        return sampledTransform
      }
      i += 1
    }
    throw new Exception("Failed to sample any valid transformations.")
  }

  /** Return an ArraySeq[Transformation] of length sampleDepth */
  @throws(classOf[Exception])
  def sample(): Array[Transformation] = {
    if (isMC) {
      randomSampleMC()
    } else {
      randomSampleRejection()
    }
  }
}

/** Class that checks whether a sampled plan returns the same result as the original instruction. */
class PlanValidator(val proposedPlan : QueryInstruction, val plan : Option[QueryInstruction] = None, val expectedResult : Option[RelationStub] = None) {
  def validate() : Boolean = {
    val expected  = if (plan.isDefined) {plan.get.execute} else {expectedResult.get}
    expected.equals(proposedPlan.execute)
  }
}