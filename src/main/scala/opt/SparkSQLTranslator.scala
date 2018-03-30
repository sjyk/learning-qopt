
package opt

import org.apache.spark.sql.catalyst.plans.logical
import dml.Join
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SparkJoin(var relations : ArrayBuffer[Either[RelationStub, QueryInstruction]],
           var parameters : ArrayBuffer[ConstraintStub], var input : logical.Join) extends QueryInstruction("join") {

  override def checkSchema(): Boolean = {
    true
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
    /* we actually want to execute the Spark Join here because there's no other way to get the relationsize of the already joined operators. */
    /* can we use the "output" list from logical.Join.output? */
    val intersection = leftTable.relationContent & rightTable.relationContent
    val joinName = rightTable.relationName + " * " + leftTable.relationName
    val cost = perJoinCost(leftTable.initCost, leftTable.relationContent.size, rightTable.initCost, rightTable.relationContent.size)
    val provenance = leftTable.provenance ++ rightTable.provenance
    new RelationStub(joinName, intersection, cost, provenance)
  }

  def perJoinCost(lCost : Double, lSize : Int, rCost : Double, rSize : Int) : Double = {
    val cost = lCost + rCost
    val IOCost = lSize * rSize
    cost + IOCost
  }

  def resolveDeepest(parent : Option[QueryInstruction] = None) : RelationStub = {
    if (this.relations(0).isLeft && this.relations(1).isLeft) {
      val result = this.execute
      if (parent.isDefined) {
        parent.get.relations(1) = Left(result)
      }
      result
    } else {
      if (this.relations(1).isRight) {
        this.relations(1).right.get.asInstanceOf[Join].resolveDeepest(Some(this))
      } else {
        throw new Exception("Left relation cannot be a QueryInstruction.")
      }
    }
  }

  override def cost : Double = {
    val joinCopy = this.deepClone.asInstanceOf[Join]
    for (i <- joinCopy.relations.indices) {
      if (joinCopy.relations(i).isRight) {
        joinCopy.relations(i) = Left(joinCopy.relations(i).right.get.execute)
      }
    }
    val left = joinCopy.relations(0).left.get
    val right = joinCopy.relations(1).left.get
    perJoinCost(left.initCost, left.relationContent.size, right.initCost, right.relationContent.size)
  }

  override def toString: String = {
    if (this.relations(0).isLeft && this.relations(1).isLeft) {
      this.relations(0).left.get.relationName + " * " + this.relations(1).left.get.relationName
    } else {
      this.relations(0).left.get.relationName + " * " + this.relations(1).right.get.toString
    }
  }
}
//
class SparkSQLTranslator {
  def sparkJoinToQIJoin(input : logical.Join) : SparkJoin = {
    if (input.resolved) {
      if (input.right.isInstanceOf[logical.Join]) {
        new SparkJoin(ArrayBuffer(Left(sparkRelationToRelationStub(input.left)), Right(sparkJoinToQIJoin(input.right.asInstanceOf[logical.Join]))), ArrayBuffer(), input)
      } else {
        new SparkJoin(ArrayBuffer(Left(sparkRelationToRelationStub(input.left)), Left(sparkRelationToRelationStub(input.right))), ArrayBuffer(), input)
      }
    } else {
      throw new Exception("Spark logical plan is not resolved")
    }
  }

  def sparkRelationToRelationStub(input : logical.LogicalPlan) : opt.RelationStub = {
    if (input.resolved) {
      if (input.isInstanceOf[logical.LocalRelation]) {
        val relation = input.asInstanceOf[logical.LocalRelation]
        // stubbing actual data with an empty Set.
        val sqlConfig = SparkSession.getActiveSession.map(_.sessionState.conf).getOrElse(SQLConf.getFallbackConf)
        return new RelationStub(relation.computeStats(sqlConfig).hashCode.toString, mutable.Set())
      }
    }
    throw new Exception("Exception trying to convert sparkRelation to RelationStub")
  }
}

// "Join" extends LogicalPlan. LogicalPlan extends QueryPlan which has an Output.
// I need a sparkJoin->RLJoin RLJoin.left is going to be a QueryPlan that masks a relation. Getting that relation involves calling output.
// I need to figure out which QueryPlans are Relations and which are Joins. That makes a QI join - the pure relations become
//