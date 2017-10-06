package dml

import opt.{ConstraintStub, QueryInstruction, RelationStub}

class sql {

}

class Join(
            instructionType : String,
            var relations : List[Either[RelationStub, QueryInstruction]],
            var parameters : List[ConstraintStub]) extends QueryInstruction(instructionType) {

  override def checkSchema(): Boolean = {
    if (relations.length > 2) {
      return false
    } else {
      return true
    }
  }

  override def execute(): RelationStub = {
    if (!checkSchema()) {
      throw new Exception("Schema validation failed for this object.")
    }
    var i = 0
    /* Resolve all Query Instructions first. */
    for (i <- 0 to relations.length) {
      val newRelation = relations(i) match {
        case Right(x) => x.execute
        case Left(x) => x
      }
      relations(i) = newRelation
    }
    /* We have 2 relation stubs - in practice, we would kick join logic to the RDD - here we compute set intersect.*/
    val leftTable = relations(0).left.getOrElse(null)
    val rightTable = relations(1).left.getOrElse(null)
      if (leftTable == null) {
      throw new Exception("Conversion to canonical relation failed.")
    }
    val intersection = leftTable.relationContent & rightTable.relationContent
    val joinName = leftTable.relationName + " * " + rightTable.relationName
    return new RelationStub(joinName, intersection)
  }
}