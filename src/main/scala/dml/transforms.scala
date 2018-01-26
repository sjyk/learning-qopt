package dml

import opt.{QueryInstruction, RelationStub, Transformation}
import learning.FeaturizationDefaults

import scala.util.Random
import scala.collection.mutable
import org.apache.spark.ml.linalg.DenseMatrix

import scala.collection.mutable.ArrayBuffer

class transforms {

}

class IdentityTransform extends Transformation {

  var input : Option[QueryInstruction] = None

  override var canonicalName: String = "Identity"

  override def transform(i : QueryInstruction, kargs : Array[Any] = Array()): QueryInstruction = {
    input = Some(i)
    i
  }

  override def featurize(trainMode : Boolean): DenseMatrix = {
    val featurization = FeaturizationDefaults.planFeaturization(input.get)._1.toArray
    new DenseMatrix(1, featurization.length, featurization)
  }
}

class JoinRandomSwap extends Transformation {

  var input : Option[QueryInstruction] = None
  var a1 : Option[RelationStub] = None
  var a2 : Option[RelationStub] = None
  var instrList : Option[Array[String]] = None
  override var canonicalName: String = "RandomSwap"

  def getRelationSet(input : QueryInstruction): mutable.Map[String, RelationStub] = {
    var relationSet = mutable.Map[String, RelationStub]()
    var allRelations = new ArrayBuffer[String]()
    for (relation <- input.relations) {
      if (relation.isLeft) {
        for (name <- relation.left.get.provenance) {
          allRelations += name
        }
        relationSet = relationSet + (relation.left.get.relationName -> relation.left.get)
      } else {
        relationSet = relationSet ++ getRelationSet(relation.right.get)
        // the result of the previous function call will put the partial global instruction list in the instrList.
        for (name <- instrList.get) {
          allRelations += name
        }
      }
    }
    val sortedRelations = allRelations.toArray.sorted
    instrList = Some(sortedRelations)
    relationSet
  }

  /** Build a set of the relations in this query plan. Pick 2 without replacement. Swap them. */
  override def transform(input: QueryInstruction, kargs : Array[Any] = Array()): QueryInstruction = {
    /* This swap is trivial and useless if there'ts only 2 relations in the table. */
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

  override def featurize(trainMode : Boolean = false): DenseMatrix = {
    val (f1, f2) = FeaturizationDefaults.joinOneHotFeaturization(a1.get, a2.get, instrList.get)
    if (trainMode) {
      new DenseMatrix(f1.size, 2, (f1 ++ f2).toArray).transpose
    } else {
      new DenseMatrix(f1.size, 1, f1.toArray).transpose
    }
  }

  override def toString: String = {
    canonicalName + "(" + a1.get.relationName + "->" + a2.get.relationName + ")"
  }
}

class RandomParallelFindMerge extends Transformation {

  var input : Option[QueryInstruction] = None

  override var canonicalName: String = "RandomParallelFindMerge"

  override def transform(i: QueryInstruction, kargs: Array[Any]): QueryInstruction = {
    input = Some(i)
    i
  }

  override def featurize(trainMode : Boolean): DenseMatrix = {
    val featurization = FeaturizationDefaults.planFeaturization(input.get)._1.toArray
    new DenseMatrix(1, featurization.length, featurization)
  }
}
