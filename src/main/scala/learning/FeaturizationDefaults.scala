package learning

import dml.Join
import opt.{ConstraintStub, QueryInstruction, RelationStub}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object BaseFeaturization {
  def getBaseSystemFeaturization: ArrayBuffer[Double] = {
    ArrayBuffer[Double]()
  }
}

/**
  * Utility class of default featurizations
  */
object FeaturizationDefaults {
  def planFeaturization(input : QueryInstruction)
  : (ArrayBuffer[Double], ArrayBuffer[Double], ArrayBuffer[Double]) = {
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

  def oneHotFeaturization(hotVector : Array[Any], globalDict : Array[Any]) : ArrayBuffer[Double] = {
    val featureVector = ArrayBuffer.fill[Double](globalDict.length)(0)
    for (n <- hotVector) {
      if (!globalDict.contains(n)) {
        throw new Exception("Attempting to set an element as hot " +
          "without it being in the dictionary.")
      }
      featureVector(globalDict.indexOf(n)) = 1
    }
    featureVector
  }

  def joinOneHotFeaturization(r1 : RelationStub,
                              r2 : RelationStub,
                              globalRelations : Array[String])
  : (ArrayBuffer[Double], ArrayBuffer[Double]) = {
    if (r1.provenance.isEmpty) {
      throw new Exception("Empty provenance for relation")
    }
    val r1Features = oneHotFeaturization(r1.provenance.toArray, globalRelations.toArray[Any])
    if (r2.provenance.isEmpty) {
      throw new Exception("Empty provenance for relation")
    }
    val r2Features = oneHotFeaturization(r2.provenance.toArray, globalRelations.toArray[Any])
    (r1Features ++ r2Features, r2Features ++ r1Features)
  }

  def joinFeaturization(r1 : RelationStub,
                        r2 : RelationStub): (ArrayBuffer[Double], ArrayBuffer[Double]) = {
    val dummyR1 = new Join(ArrayBuffer[Either[RelationStub, QueryInstruction]](Left(r1)),
      ArrayBuffer[ConstraintStub]())
    val r1Features = planFeaturization(dummyR1)._2
    val dummyR2 = new Join(ArrayBuffer[Either[RelationStub, QueryInstruction]](Left(r2)),
      ArrayBuffer[ConstraintStub]())
    val r2Features = planFeaturization(dummyR2)._2
    (r1Features ++ r2Features, r2Features ++ r1Features)
  }

  def findReplaceFeaturization(QIMap : ArrayBuffer[QueryInstruction]) : ArrayBuffer[Double] = {
    var max = 0
    var rNameList = mutable.Set[String]()
    var constraintStrings = mutable.Set[String]()
    var attrList = mutable.Set[Int]()
    for (i <- QIMap) {
      if (i.parameters.length > max) {
        max = i.parameters.length
      }
      rNameList += i.relations(0).left.get.relationName
      for (p <- i.parameters) {
        for (c <- p.constraints) {
          if (c.isLeft) {
            attrList += c.left.get
          } else {
            constraintStrings += c.right.get
          }
        }
      }
    }
    val rNameVSize = rNameList.size
    val cStringVSize = constraintStrings.size
    val attrVSize = attrList.size
    var relationFeature = ArrayBuffer.fill[Double](
      (rNameVSize + max * (cStringVSize * 2 + attrVSize)) * QIMap.size)(0)
    var idx = 0
    for (i <- QIMap.indices) {
      val qi = QIMap(i)
      var relationIdx = i * (1 + max*3)
      val rNameFeature = oneHotFeaturization(
        Array(qi.relations(0).left.get.relationName),
        rNameList.toArray)
      for (x <- rNameFeature) {
        relationFeature(relationIdx) = x
        relationIdx += 1
      }
      for (c <- qi.parameters) {
        for (item <- c.constraints) {
          if (item.isRight) {
            val cStringFeature = oneHotFeaturization(
              Array(item.right.get),
              constraintStrings.toArray)
            for (x <- cStringFeature) {
              relationFeature(relationIdx) = x
              relationIdx += 1
            }
          } else {
            val attrFeature = oneHotFeaturization(Array(item.left.get), attrList.toArray)
            for (x <- attrFeature) {
              relationFeature(relationIdx) = x
              relationIdx += 1
            }
          }
        }
      }
    }
    relationFeature
  }
}}