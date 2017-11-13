package learning

import dml.Join
import opt.{ConstraintStub, QueryInstruction, RelationStub}

import scala.collection.mutable.ArrayBuffer


object BaseFeaturization {
  def getBaseSystemFeaturization: ArrayBuffer[Int] = {
    ArrayBuffer[Int]()
  }
}

/**
  * Utility class of default featurizations
  */
object FeaturizationDefaults {
  def planFeaturization(input : QueryInstruction) : (ArrayBuffer[Double], ArrayBuffer[Double], ArrayBuffer[Double]) = {
    // we cannot featurize the relations - they vary in size and may be tremendous in size.
    // we *can* featurize the cardinality.
    var featureVector = new ArrayBuffer[Double]()
    var relationVector = new ArrayBuffer[Double]()
    var constraintVector = new ArrayBuffer[Double]()
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

  // use isCommutative flag
  // use isAssociative?
  /* Global list is a parameter in case we want to do a one-hot featurization */
  def joinFeaturization(r1 : RelationStub, r2 : RelationStub, globalRelations : Vector[String]): (ArrayBuffer[Double], ArrayBuffer[Double]) = {
    val dummyR1 = new Join(ArrayBuffer[Either[RelationStub, QueryInstruction]](Left(r1)), ArrayBuffer[ConstraintStub]())
    val r1Features = planFeaturization(dummyR1)._2
    val dummyR2 = new Join(ArrayBuffer[Either[RelationStub, QueryInstruction]](Left(r2)), ArrayBuffer[ConstraintStub]())
    val r2Features = planFeaturization(dummyR2)._2
    (r1Features ++ r2Features, r2Features ++ r1Features)
  }
}