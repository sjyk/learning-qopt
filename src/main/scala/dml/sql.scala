package dml

import opt.{ConstraintStub, QueryInstruction, RelationStub}

import scala.collection.mutable.ArrayBuffer

class sql {

}

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
    val leftTable = relations(0).left.getOrElse(null)
    val rightTable = relations(1).left.getOrElse(null)
      if (leftTable == null) {
      throw new Exception("Conversion to canonical relation failed.")
    }
    val intersection = leftTable.relationContent & rightTable.relationContent
    val joinName = leftTable.relationName + " * " + rightTable.relationName
    new RelationStub(joinName, intersection)
  }

  override def cost: Double = {
    var cost = 0.0
    var joinCopy = this.deepClone.asInstanceOf[Join]
    for (relation <- joinCopy.relations) {
      if (relation.isRight) {
        cost += relation.right.get.cost
      } else {
        cost += relation.left.get.initCost
      }
    }
    var ioCost = joinCopy.relations(0).left.get.relationContent.size
    for (i <- 1 until joinCopy.relations.size) {
      var relation = joinCopy.relations(i)
      if (relation.isRight) {
        cost *= relation.right.get.execute.relationContent.size
      }
    }
    cost += ioCost
    cost
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