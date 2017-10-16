package dml

import learning.PlanValidator
import opt.{ConstraintStub, QueryInstruction, RelationStub}

import scala.collection.mutable.{ArrayBuffer, ArraySeq}

class sql {

}

class Join(var relations : ArrayBuffer[Either[RelationStub, QueryInstruction]],
           var parameters : ArrayBuffer[ConstraintStub]) extends QueryInstruction("join") {

  override def checkSchema(): Boolean = {
    if (relations.length > 2) {
      false
    } else {
      true
    }
  }

  override def execute(): RelationStub = {
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
}
