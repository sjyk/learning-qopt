package dml

import opt.{QueryInstruction, Transformation}
import learning.FeaturizationDefaults
import scala.util.Random

import scala.collection.mutable

class transforms {

}

class IdentityTransform extends Transformation {
  override def transform(input : QueryInstruction, kargs : Array[Any] = Array()): QueryInstruction = {
    input
  }

  override def featurize(input : QueryInstruction): Vector[Int] = {
    FeaturizationDefaults.planFeaturization(input)._1.toVector
  }
}


class OtherTransform extends Transformation {
  override def transform(input : QueryInstruction, kargs : Array[Any] = Array()): QueryInstruction = {
    input
  }

  override def featurize(input : QueryInstruction): Vector[Int] = {
    FeaturizationDefaults.planFeaturization(input)._1.toVector
  }
}

class JoinRandomSwap extends Transformation {
  def getRelationSet(input : QueryInstruction): mutable.Set[String] = {
    val relationSet = mutable.Set[String]()
    for (relation <- input.relations) {
      if (relation.isLeft) {
        relationSet + relation.left.get.relationName
      } else {
        relationSet ++ getRelationSet(relation.right.get)
      }
    }
    relationSet
  }

  /** Build a set of the relations in this query plan. Pick 2 without replacement. Swap them. */
  override def transform(input: QueryInstruction, kargs : Array[Any] = Array()): QueryInstruction = {
    /* This swap is trivial and useless if there's only 2 relations in the table. */
    val relationSet = getRelationSet(input).toVector
    val r1 = Random.shuffle(relationSet).asInstanceOf[Vector[String]](0)
    var foundPair = false
    var r2 = ""
    while (!foundPair) {
      r2 = Random.shuffle(relationSet).asInstanceOf[Vector[String]](0)
      if (r1 != r2) {
        foundPair = true
      }
    }
    var foundFirst = false
    var foundSecond = false
    var instrRef = input
    var firstRef : QueryInstruction = null
    var secondRef : QueryInstruction = null
    /* We will make the assumption that the left relation (relations(0)) of the plan will be a single, non-joined
     * relation with its original name. */
    var curRelationName = instrRef.relations(0).left.get.relationName
    while (!foundFirst && !foundSecond) {
      if (curRelationName == r1) {
        firstRef = instrRef
        foundFirst = true
      } else if (curRelationName == r2) {
        secondRef = instrRef
        foundSecond = true
      }
      if (instrRef.relations(1).isRight) {
        instrRef = instrRef.relations(1).right.get
        curRelationName = instrRef.relations(0).left.get.relationName
      } else {
        curRelationName = instrRef.relations(1).left.get.relationName
      }
    }
    /* swap the relations */
    val tmpRel = firstRef.relations(0)
    firstRef.relations(0) = secondRef.relations(0)
    secondRef.relations(0) = tmpRel
    input
  }

  // featurize as the cardinality of the intermediate products?
  override def featurize(input: QueryInstruction): Vector[Int] = {
    FeaturizationDefaults.planFeaturization(input)._1.toVector
  }
}

class RandomParallelFindMerge extends Transformation {
  override def transform(input: QueryInstruction, kargs: Array[Any]): QueryInstruction = {
    input
  }

  override def featurize(input: QueryInstruction): Vector[Int] = {
    FeaturizationDefaults.planFeaturization(input)._1.toVector
  }
}
