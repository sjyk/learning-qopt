package dml

import opt.{ConstraintStub, QueryInstruction, RelationStub}

import scala.collection.mutable.ArrayBuffer

class Join(var relations : ArrayBuffer[Either[RelationStub, QueryInstruction]],
           var parameters : ArrayBuffer[ConstraintStub]) extends QueryInstruction("join") {

  override def checkSchema(): Boolean = {
    if (relations.size > 2) {
      false
    } else {
      true
    }
  }

  override def execute: RelationStub = {
    if (!checkSchema()) {
      throw new Exception("Schema validation failed for this object.")
    }
    var i = 0
    /* Resolve all Query Instructions first. */
    for (i <- relations.indices) {
      val newRelation = relations(i) match {
        case Right(x) => x.execute
        case Left(x) => x
      }
      relations.update(i, Left(newRelation))
    }
    /* We have 2 relation stubs - in practice, we would kick join logic to the RDD - here we compute set intersect.*/
    val leftTable = relations(0).left.get
    val rightTable = relations(1).left.get
    val intersection = leftTable.relationContent & rightTable.relationContent
    val joinName = rightTable.relationName + " * " + leftTable.relationName
    val cost = perJoinCost(leftTable.initCost, leftTable.relationContent.size, rightTable.initCost, rightTable.relationContent.size)
    new RelationStub(joinName, intersection, cost)
  }

  def perJoinCost(lCost : Double, lSize : Int, rCost : Double, rSize : Int) : Double = {
    val cost = lCost + rCost
    val IOCost = lSize * rSize
    cost + IOCost
  }

  override def cost : Double = {
    val joinCopy = this.deepClone.asInstanceOf[Join]
    println(joinCopy.toString)
    for (i <- joinCopy.relations.indices) {
      if (joinCopy.relations(i).isRight) {
        joinCopy.relations(i) = Left(joinCopy.relations(i).right.get.execute)
      }
    }
    val left = joinCopy.relations(0).left.get
    val right = joinCopy.relations(1).left.get
    perJoinCost(left.initCost, left.relationContent.size, right.initCost, right.relationContent.size)
  }
}

object JoinUtils {
  def initJoinFromList(relations : ArrayBuffer[RelationStub]) : Join = {
    if (relations.size == 2) {
      new Join(ArrayBuffer[Either[RelationStub, QueryInstruction]](Left(relations(0)), Left(relations(1))), ArrayBuffer[ConstraintStub]())
    } else {
      val transformedRelations = ArrayBuffer[Either[RelationStub, QueryInstruction]](Left(relations(0)), Right(initJoinFromList(relations.slice(1, relations.size))))
      val params = ArrayBuffer[ConstraintStub]()
      new Join(transformedRelations, params)
    }
  }
}