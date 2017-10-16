package opt

import scala.collection.mutable.{ArrayBuffer, Set}

/* Set of allowed instructions */
object validInstructionTypes {
  val validTypes : Seq[String] = Seq("join", "find")
}

/* Stub classes for relations and constraints - these will need to be customized into the Spark type we want. */
class RelationStub(var relationName : String, var relationContent : Set[Array[String]]) {
  override def toString: String = {
    relationName + ", content: " + relationContent.toString()
  }
}

class ConstraintStub(var constraints : ArrayBuffer[Either[Int, String]])

/* QueryInstruction is an abstract class that can be implemented by any given instruction (like join). Instructions
 * must naturally contain a set of relations that are either a table or another, subsequent instruction's intermediate
 * result.
 */
abstract class QueryInstruction(var instructionType : String) {
  require(validInstructionTypes.validTypes.contains(instructionType), "Invalid instruction type")
  /* Depending on the instruction, different relations popped off for use.
     In a join, Join(A, Join(B,C)) stores [A, Join(B, C)] and pops off these two to perform the operation
   */
  var relations : ArrayBuffer[Either[RelationStub, QueryInstruction]]
  var parameters : ArrayBuffer[ConstraintStub]

  /** Store a set of all possible transformations allowed on ANY queryinstruction of this type. */
  var allowedTransformations : Option[Set[Transformation]] = None
  /** Defines a function that takes in a QueryInstruction and defines logic to return possible transforms on this.
    * If you configure this function, you MUST also define allowedTransformations. */
  def getAllowedTransformations(query : QueryInstruction) : Set[Transformation] = {
    Set()
  }

  /* This function must be implemented to check that an instruction has valid relationships.
     For example, in join - there can only be 2 relations in the relation list.
   */
  def checkSchema() : Boolean

  @throws(classOf[Exception])
  def execute : RelationStub

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[QueryInstruction]) {
      return false
    }
    val otherInstruction = obj.asInstanceOf[QueryInstruction]
    if (!otherInstruction.instructionType.equals(instructionType)) {
      return false
    } else if (!(otherInstruction.parameters.equals(parameters) && otherInstruction.relations.equals(relations))) {
      return false
    }
    true
  }

  override def toString: String = {
    "(" + instructionType + " " + relations.toString() + "," + parameters.toString() + ")"
  }

  override def clone(): QueryInstruction = {

  }
}

/* Utility class to contain the metadata about a query plan and the root instruction for the plan */
trait QueryPlan {
  var totalCost : Integer
  var planRoot : QueryInstruction
}

/* Transformations convert 1 instruction to another instruction. */
abstract class Transformation {
  def transform(input : QueryInstruction) : QueryInstruction
}
