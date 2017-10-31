package learning

import opt.QueryInstruction
import scala.collection.mutable.ArrayBuffer

/**
  * Utility class of default featurizations
  */
object FeaturizationDefaults {
  def planFeaturization(input : QueryInstruction) : (ArrayBuffer[Int], ArrayBuffer[Int], ArrayBuffer[Int]) = {
    // we cannot featurize the relations - they vary in size and may be tremendous in size.
    // we *can* featurize the cardinality.
    var featureVector = new ArrayBuffer[Int]()
    var relationVector = new ArrayBuffer[Int]()
    var constraintVector = new ArrayBuffer[Int]()
    for (relation <- input.relations) {
      if (relation.isLeft) {
        relationVector += relation.left.get.relationContent.size
      } else {
        val (_, rv, cv) = planFeaturization(relation.right.get)
        relationVector = relationVector ++ rv
        constraintVector = constraintVector ++ cv
      }
    }
    for (constraint <- input.parameters) {
      for (constraintString <- constraint.constraints) {
        if (constraintString.isRight) {
          constraintVector += constraintString.right.get.hashCode
        } else {
          constraintVector += constraintString.left.get
        }
      }
    }
    featureVector = relationVector ++ constraintVector
    (featureVector, relationVector, constraintVector)
  }
}