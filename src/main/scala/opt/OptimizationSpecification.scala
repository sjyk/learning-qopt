package opt

import scala.collection.mutable.{ArrayBuffer, Set}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream}

import org.nd4j.linalg.api.ndarray.INDArray

/** Set of allowed instructions */
object validInstructionTypes {
  val validTypes : Seq[String] = Seq("join", "find")
}

/**
  * Stub classes for relations and constraints -
  * these will need to be customized into the Spark type we want.
  */
// scalastyle:off covariant.equals equals.hash.code
@SerialVersionUID(2L)
class RelationStub(var relationName : String,
                   var relationContent : Set[Seq[String]],
                   var initCost : Double = 0,
                   var provenance : ArrayBuffer[String] = ArrayBuffer()) extends Serializable {
  if (provenance.isEmpty) {
    provenance = ArrayBuffer(relationName)
  }
  override def toString: String = {
    relationName + ", cost:" + initCost.toString
  }

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[RelationStub]) {
      return false
    }
    val otherRelation = obj.asInstanceOf[RelationStub]
    // Name and initCost aren't stable checks for equality.
    otherRelation.relationContent.equals(relationContent)
  }
}

@SerialVersionUID(2L)
class ConstraintStub(var constraints : ArrayBuffer[Either[Int, String]]) extends Serializable

// scalastyle:off covariant.equals equals.hash.code
/** QueryInstruction is an abstract class that can be implemented
  * by any given instruction (like join). Instructions must naturally
  * contain a set of relations that are either a table or another,
  * subsequent instruction's intermediate result.  */
@SerialVersionUID(2L)
abstract class QueryInstruction(var instructionType : String) extends Serializable {
  require(validInstructionTypes.validTypes.contains(instructionType), "Invalid instruction type")
  /** Depending on the instruction, different relations are popped off for use.
    * In a join, Join(A, Join(B,C)) stores [A, Join(B, C)] and
    * pops off these two to perform the operation
    */
  var relations : ArrayBuffer[Either[RelationStub, QueryInstruction]]
  var parameters : ArrayBuffer[ConstraintStub]

  /** Defines a function that takes in a QueryInstruction
    * and defines logic to return possible transforms on this.
    * If you configure this function, you MUST also define allowedTransformations. */
  def getAllowedTransformations(query : QueryInstruction) : Set[Transformation] = {
    throw new NotImplementedError("No implementation provided for allowed transformation method.")
  }

  /** This function must be implemented to check that an instruction has valid relationships.
     For example, in join - there can only be 2 relations in the relation list.
    */
  def checkSchema() : Boolean

  @throws(classOf[Exception])
  def execute : RelationStub

  def cost : Double

  // scalastyle:off println
  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[QueryInstruction]) {
      println("This isn't a QI")
      return false
    }
    val otherInstruction = obj.asInstanceOf[QueryInstruction]
    if (!otherInstruction.instructionType.equals(instructionType)) {
      println("Invalid type parameter")
      return false
    } else if (!(otherInstruction.parameters.equals(parameters) && otherInstruction.relations.equals(relations))) {
      println("params or relations don't match")
      return false
    }
    true
  }
  // scalastyle:on println

  override def toString: String = {
    "(" + instructionType + " " + relations.toString() + "," + parameters.toString() + ")"
  }

  def deepClone: QueryInstruction = try {
    val outStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(outStream)
    oos.writeObject(this)
    val inStream = new ByteArrayInputStream(outStream.toByteArray)
    val ois = new ObjectInputStream(inStream)
    val returnedObj = ois.readObject
    returnedObj.asInstanceOf[QueryInstruction]
  } catch {
    case e: IOException =>
      null
    case e: ClassNotFoundException =>
      null
  }
}
// scalastyle:on covariant.equals equals.hash.code

/** Utility class to contain the metadata about a query plan
  * and the root instruction for the plan */
trait QueryPlan {
  var totalCost : Integer
  var planRoot : QueryInstruction
}

/** Transformations convert 1 instruction to another instruction. */
@SerialVersionUID(2L)
abstract class Transformation extends Serializable {

  var input : Option[QueryInstruction]

  var canonicalName : String

  def transform(input : QueryInstruction, kargs : Array[Any] = Array()) : QueryInstruction

  def featurize(trainMode : Boolean = false) : INDArray

  override def toString: String = {
    canonicalName
  }

  def deepClone: Transformation = try {
    val outStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(outStream)
    oos.writeObject(this)
    val inStream = new ByteArrayInputStream(outStream.toByteArray)
    val ois = new ObjectInputStream(inStream)
    val returnedObj = ois.readObject
    returnedObj.asInstanceOf[Transformation]
  } catch {
    case e: IOException =>
      null
    case e: ClassNotFoundException =>
      null
  }
}
