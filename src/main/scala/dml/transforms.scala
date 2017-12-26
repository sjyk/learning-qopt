package dml

import opt.{QueryInstruction, RelationStub, Transformation}
import learning.FeaturizationDefaults

import scala.util.Random
import scala.collection.mutable
import org.apache.spark.ml.linalg.DenseMatrix

import scala.util.control.Exception

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
    var foundFirst = false
    var foundSecond = false
    var instrRef = input
    var firstRef : Option[QueryInstruction] = None
    var firstRelRef : Option[RelationStub] = None
    var firstIdx : Int = 0
    var secondRef : Option[QueryInstruction] = None
    var secondRelRef : Option[RelationStub] = None
    var secondIdx : Int = 0
    /* We will make the assumption that the left relation (relations(0)) of the plan will be a single, non-joined
     * relation with its original name. */
    var curRelation = instrRef.relations(0).left.get
    var idx = 0
    while (!(foundFirst && foundSecond)) {
      if (curRelation.relationName == r1Name) {
        firstRef = Some(instrRef)
        firstRelRef = Some(curRelation)
        firstIdx = idx
        foundFirst = true
      } else if (curRelation.relationName == r2Name) {
        secondRef = Some(instrRef)
        secondRelRef = Some(curRelation)
        secondIdx = idx
        foundSecond = true
      }
      if (instrRef.relations(1).isRight) {
        instrRef = instrRef.relations(1).right.get
        curRelation = instrRef.relations(0).left.get
        idx = 0
      } else {
        curRelation = instrRef.relations(1).left.get
        idx = 1
      }
    }
    /* swap the relations */
    firstRef.get.relations(firstIdx) = Left(secondRelRef.get)
    secondRef.get.relations(secondIdx) = Left(firstRelRef.get)
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
