package learning

import opt.{QueryInstruction, RelationStub, Transformation}
import dml.{IdentityTransform, Join, JoinRandomSwap}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Random, Success, Try}

/** This class needs to sample from a space of possible outcomes.
  * We allow for two forms of sampling - the first is Monte-Carlo sampling, where the implementer of the instruction
  * defines a function getAllowedTransformations on a given plan. This is significant overhead to the implementer, but
  * more efficient to sample fast. Rejection sampling is MC sampling with all the actions as allowed transformations,
  * and the sampler will default to this if the instruction does not implement the allowed transformations API.
  */
class Sampler(var initialPlan : QueryInstruction, var sampleDepth : Int, var withValidation : Boolean = true, var maxAttempts : Option[Int] = None) {

  val isMC : Boolean = {
    val accessor = Try(initialPlan.getAllowedTransformations(initialPlan))
    accessor match {
      case Failure(thrown) => {
        false
      }
      case Success(s) => {
        true
      }
    }
  }
  val allTransformations : Vector[Transformation] = mutable.Set[Transformation](new JoinRandomSwap).toVector
  val transformationMaxAttempts : Int = maxAttempts.getOrElse(1000)

  @throws(classOf[Exception])
  private def randomSampleMC() : (Array[Transformation], QueryInstruction) = {
    val rnd = new Random()
    val transformationList = new Array[Transformation](sampleDepth)
    val pType = initialPlan.getClass
    var planCopy = initialPlan.deepClone
    for (i <- transformationList.indices) {
      val tSet = initialPlan.getAllowedTransformations(planCopy)
      val randomTransform = tSet.toVector(rnd.nextInt(tSet.size))
      transformationList.update(i, randomTransform)
      planCopy = randomTransform.transform(planCopy)
    }
    if (withValidation && (!PlanValidator.validate(planCopy, Some(initialPlan)))) {
      throw new Exception("MC sampler has generated an invalid plan.")
    }
    (transformationList, planCopy)
  }

  @throws(classOf[Exception])
  private def randomSampleRejection() : (Array[Transformation], QueryInstruction) = {
    var i = 0
    while (i < transformationMaxAttempts) {
      /** Sample a transformation order */
      val sampledTransform = Array.fill(sampleDepth){allTransformations(scala.util.Random.nextInt(allTransformations.size)).deepClone}
      var planCopy = initialPlan.deepClone
      for (x <- sampledTransform) {
        planCopy = x.transform(planCopy)
        planCopy.asInstanceOf[Join].resolveDeepest()
      }
      if (withValidation && PlanValidator.validate(planCopy, Some(initialPlan))) {
        return (sampledTransform, planCopy)
      } else if (!withValidation) {
        return (sampledTransform, planCopy)
      }
      i += 1
    }
    throw new Exception("Failed to sample any valid transformations.")
  }

  /** Return an ArraySeq[Transformation] of length sampleDepth */
  @throws(classOf[Exception])
  def sample(): (Array[Transformation], QueryInstruction) = {
    if (isMC) {
      randomSampleMC()
    } else {
      randomSampleRejection()
    }
  }

  def sampleN(n : Int): (Array[Array[Transformation]], Array[QueryInstruction]) = {
    var sampledTransforms = Vector[Array[Transformation]]()
    var sampledInstructions = Vector[QueryInstruction]()
    for (i <- 1 to n) {
      val (tList, instr) = sample()
      sampledTransforms = sampledTransforms :+ tList
      sampledInstructions = sampledInstructions :+ instr
    }
    (sampledTransforms.toArray, sampledInstructions.toArray)
  }
}

/** Class that checks whether a sampled plan returns the same result as the original instruction. */
object PlanValidator {
  def validate(proposedPlan : QueryInstruction, plan : Option[QueryInstruction] = None, expectedResult : Option[RelationStub] = None) : Boolean = {
    val expected  = if (plan.isDefined) {plan.get.deepClone.execute} else {expectedResult.get}
    expected.equals(proposedPlan.deepClone.execute)
  }
}
