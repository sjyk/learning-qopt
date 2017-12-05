package dml

import opt.{QueryInstruction, RelationStub, Transformation}
import learning.FeaturizationDefaults

import scala.util.Random
import scala.collection.mutable
import org.apache.spark.ml.linalg.DenseMatrix

class transforms {

}

class IdentityTransform extends Transformation {

  var input : Option[QueryInstruction] = None

  override var canonicalName: String = "Identity"

  override def transform(i : QueryInstruction, kargs : Array[Any] = Array()): QueryInstruction = {
    input = Some(i)
    i
  }

  override def featurize: DenseMatrix = {
    val featurization = FeaturizationDefaults.planFeaturization(input.get)._1.toArray
    new DenseMatrix(1, featurization.length, featurization)
  }
}

class JoinRandomSwap extends Transformation {

  var input : Option[QueryInstruction] = None
  var a1 : Option[RelationStub] = None
  var a2 : Option[RelationStub] = None
  var instrList : Option[Vector[String]] = None
  override var canonicalName: String = "RandomSwap"

  def getRelationSet(input : QueryInstruction): mutable.Map[String, RelationStub] = {
    var relationSet = mutable.Map[String, RelationStub]()
    for (relation <- input.relations) {
      if (relation.isLeft) {
        relationSet = relationSet + (relation.left.get.relationName -> relation.left.get)
      } else {
        relationSet = relationSet ++ getRelationSet(relation.right.get)
      }
    }
    val sortedRelations = relationSet.keys.toVector.sorted
    instrList = Some(sortedRelations)
    relationSet
  }

  /** Build a set of the relations in this query plan. Pick 2 without replacement. Swap them. */
  override def transform(input: QueryInstruction, kargs : Array[Any] = Array()): QueryInstruction = {
    /* This swap is trivial and useless if there's only 2 relations in the table. */
    println("Before swap:")
    println(input)
    val relationSet = getRelationSet(input).toVector
    val r1Obj = Random.shuffle(relationSet).asInstanceOf[Vector[(String, RelationStub)]](0)
    val r1Name = r1Obj._1
    a1 = Some(r1Obj._2)
    var foundPair = false
    var r2Name = ""
    while (!foundPair) {
      val r2_obj = Random.shuffle(relationSet).asInstanceOf[Vector[(String, RelationStub)]](0)
      r2Name = r2_obj._1
      if (r1Name != r2Name) {
        foundPair = true
        a2 = Some(r2_obj._2)
      }
    }
    println(r1Name, r2Name)
    var foundFirst = false
    var foundSecond = false
    var instrRef = input
    var firstRef : QueryInstruction = null
    var secondRef : QueryInstruction = null
    /* We will make the assumption that the left relation (relations(0)) of the plan will be a single, non-joined
     * relation with its original name. */
    var curRelationName = instrRef.relations(0).left.get.relationName
    while (!(foundFirst && foundSecond)) {
      if (curRelationName == r1Name) {
        firstRef = instrRef
        foundFirst = true
      } else if (curRelationName == r2Name) {
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
    /* bug here */
    /* swap the relations */
    val tmpRel = firstRef.relations(0)
    firstRef.relations(0) = secondRef.relations(0)
    secondRef.relations(0) = tmpRel
    println("After swap:")
    println(input)
    input
  }

  override def featurize: DenseMatrix = {
    // we know what was selected.
    val (f1, f2) = FeaturizationDefaults.joinFeaturization(a1.get, a2.get, instrList.get)
    new DenseMatrix(2, f1.size, (f1 ++ f2).toArray)
  }
}

class RandomParallelFindMerge extends Transformation {

  var input : Option[QueryInstruction] = None

  override var canonicalName: String = "RandomParallelFindMerge"

  override def transform(i: QueryInstruction, kargs: Array[Any]): QueryInstruction = {
    input = Some(i)
    i
  }

  override def featurize: DenseMatrix = {
    val featurization = FeaturizationDefaults.planFeaturization(input.get)._1.toArray
    new DenseMatrix(1, featurization.length, featurization)
  }
}
