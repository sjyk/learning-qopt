package dml

import opt.{ConstraintStub, QueryInstruction, RelationStub}

import scala.collection.mutable.{ArrayBuffer, ArraySeq}

class dc {

}

class DataRelationStub(var dataRelationName : String,
                       var dataRelationContent : Array[Array[String]]) extends RelationStub(dataRelationName, Set())

class Find(var relations : ArrayBuffer[Either[RelationStub, QueryInstruction]],
           var parameters : ArrayBuffer[ConstraintStub]) extends QueryInstruction("find") {

  override def checkSchema(): Boolean = {
    if ((relations.length == 1) && (parameters.length == 1)) {
      for (p <- parameters) {
        if (!checkValidConstraint(p)) {
          throw new Exception("Invalid parameter")
        }
      }
      return true
    }
    false
  }

  def checkValidConstraint(constraintStub: ConstraintStub): Boolean = {
    if (constraintStub.constraints.length != 3) {
      return false
    }
    if (constraintStub.constraints(0).isLeft) {
      /* the string to find must be a Right (or a string) */
      return false
    } else if (constraintStub.constraints(1).isLeft) {
      return false
    } else if (constraintStub.constraints(2).isRight) {
      return false
    }
    true
  }

  override def execute: DataRelationStub = {
    if (!checkSchema()) {
      throw new Exception("Schema check failed")
    }
    val relation = if (relations(0).isRight) {
      relations(0).right.get.execute.asInstanceOf[DataRelationStub]
    } else {
      relations(0).left.get.asInstanceOf[DataRelationStub]
    }
    relations(0) = Left(relation)
    /* if parameters.length is > 1, that means that we have combined multiple sets of constraints into 1 */
    for (paramSet <- parameters) {
      val find = paramSet.constraints(0).right.getOrElse("")
      val replace = paramSet.constraints(1).right.getOrElse("")
      val onAttr = paramSet.constraints(2).left.getOrElse(-1)
      for (row <- relation.dataRelationContent) {
        if (row(onAttr) == find) {
          row.update(onAttr, replace)
        }
      }
    }
    relation
  }
}
