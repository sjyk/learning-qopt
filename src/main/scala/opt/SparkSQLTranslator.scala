
package opt

import org.apache.spark.sql.catalyst.plans.{Inner, logical}
import org.apache.spark.sql.catalyst.analysis
import dml.Join
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SparkRelationStub(var tableName : String,
                        var sparkSession : SparkSession,
                        var correspondingLogicalPlan : LogicalPlan,
                        var evaluatedTable : Option[Dataset[Row]] = None,
                        var cost : Double = 0,
                        var p : ArrayBuffer[String] = ArrayBuffer())
  extends RelationStub(tableName, mutable.Set(), cost, p) {
  var count : Option[Long] = None
  def getCount : Long = {
    if (count.isEmpty) {
      if (evaluatedTable.isDefined) {
        count = Some(evaluatedTable.get.count())
      } else {
        val qe = sparkSession.sessionState.executePlan(correspondingLogicalPlan)
        qe.assertAnalyzed()
        val table_rows = new Dataset[Row](
          sparkSession,
          correspondingLogicalPlan,
          RowEncoder(qe.analyzed.schema)).count()
        count = Some(table_rows)
      }
    }
    count.get
  }
}

class SparkJoin(var relations : ArrayBuffer[Either[RelationStub, QueryInstruction]],
                var parameters : ArrayBuffer[ConstraintStub],
                var input : logical.LogicalPlan,
                var sparkSession : SparkSession) extends QueryInstruction("join") {

  var result : Option[Dataset[Row]] = None

  override def checkSchema(): Boolean = {
    true
  }

  override def execute: RelationStub = {
    if (!checkSchema()) {
      throw new Exception("Schema validation failed for this object.")
    }
    val translator = new SparkSQLTranslator(sparkSession)
    var joinLogicalPlan = translator.queryInstructionToLogicalPlan(this)
    var i = 0
    /* Resolve all Query Instructions first. */
    for (i <- relations.indices) {
      val newRelation = relations(i) match {
        case Right(x) => x.execute
        case Left(x) => x
      }
      relations.update(i, Left(newRelation))
    }
    val qe = sparkSession.sessionState.executePlan(joinLogicalPlan)
    qe.assertAnalyzed()
    if (result.isEmpty) {
      var table = new Dataset[Row](sparkSession, joinLogicalPlan, RowEncoder(qe.analyzed.schema))
      result = Some(table)
    }
    val leftTable = relations(0).left.get
    val rightTable = relations(1).left.get
    val joinName = rightTable.relationName + " * " + leftTable.relationName
    val lTableRowSize = leftTable.asInstanceOf[SparkRelationStub].getCount
    val rTableRowSize = rightTable.asInstanceOf[SparkRelationStub].getCount
    val cost = perJoinCost(leftTable.initCost, lTableRowSize, rightTable.initCost, rTableRowSize)
    val provenance = leftTable.provenance ++ rightTable.provenance
    new SparkRelationStub(joinName, sparkSession, joinLogicalPlan, result, cost, provenance)
  }

  def perJoinCost(lCost : Double, lSize : Long, rCost : Double, rSize : Long) : Double = {
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
    perJoinCost(left.initCost,
      left.relationContent.size,
      right.initCost,
      right.relationContent.size)
  }

  override def toString: String = {
    if (this.relations(0).isLeft && this.relations(1).isLeft) {
      this.relations(0).left.get.relationName + " * " + this.relations(1).left.get.relationName
    } else {
      this.relations(0).left.get.relationName + " * " + this.relations(1).right.get.toString
    }
  }
}

class SparkSQLTranslator(var sparkSession: SparkSession,
                         var catalog: Option[Catalog] = None,
                         var sqlConfig: Option[SQLConf] = None) {
  var relationReference : mutable.Map[String, UnresolvedRelation] =
    mutable.Map[String, UnresolvedRelation]()
  var parentLogicalPlan : Option[logical.LogicalPlan] = None
  var translatedChildIndex : Option[Int] = None

  def logicalPlanToQI(input : logical.LogicalPlan,
                      catalog : Catalog,
                      sqlConf: SQLConf) : Either[RelationStub, QueryInstruction] = {
    if (input.isInstanceOf[logical.Join]) {
      val joinPlan = input.asInstanceOf[BinaryNode]
      // either it's a [Join, Relation] or a [Relation, Relation]
      if (joinPlan.left.isInstanceOf[logical.Join]) {
        val rightPlan = logicalPlanToQI(joinPlan.right, catalog, sqlConf)
        if (rightPlan.isRight) {
          throw new Exception("Unexpected: Right plan of a join is a QueryInstruction")
        }
        val leftPlan = logicalPlanToQI(joinPlan.left, catalog, sqlConf)
        if (leftPlan.isLeft) {
          throw new Exception("Unexpected: Left plan of a join is a RelationStub")
        }
        Right(new SparkJoin(
          ArrayBuffer(
            Left(rightPlan.left.get),
            Right(leftPlan.right.get)),
          ArrayBuffer(),
          input,
          sparkSession))
      } else {
        Right(new SparkJoin(
          ArrayBuffer(
            Left(logicalPlanToQI(joinPlan.left, catalog, sqlConf).left.get),
            Left(logicalPlanToQI(joinPlan.right, catalog, sqlConf).left.get)),
          ArrayBuffer(),
          input,
          sparkSession))
      }
    } else if (input.isInstanceOf[UnresolvedRelation]) {
      Left(sparkRelationToRelationStub(input, sqlConf))
    } else {
      for (i <- input.children.indices) {
        var c = input.children(i)
        if (c.isInstanceOf[logical.Join]) {
          parentLogicalPlan = Some(input)
          translatedChildIndex = Some(i)
          return logicalPlanToQI(c, catalog, sqlConf)
        }
      }
      // if it's some other instruction, just give up and stub it as a relation with no data.
      Left(new RelationStub(input.getClass.getName, mutable.Set()))
    }
  }

  def sparkRelationToRelationStub(input : logical.LogicalPlan,
                                  sqlConf : SQLConf) : RelationStub = {
    if (input.isInstanceOf[analysis.UnresolvedRelation]) {
      val relation = input.asInstanceOf[analysis.UnresolvedRelation]
      // stubbing actual data with an empty Set.
      val tableName = relation.tableName
      relationReference = relationReference + (tableName -> relation)
      new RelationStub(tableName, mutable.Set())
    } else {
      throw new Exception("Trying to convert a non-LocalRelation object to RelationStub.")
    }
  }

  def relationStubToSparkRelation(input : RelationStub) : Option[UnresolvedRelation] = {
    relationReference get input.relationName
  }

  def queryInstructionToLogicalPlan(input : QueryInstruction): logical.LogicalPlan = {
    // probably have to store some prelude to the actual joins to execute this.
    if (input.isInstanceOf[Join]) {
      val join = input.asInstanceOf[SparkJoin]
      if (join.relations(0).isLeft && join.relations(1).isLeft) {
        val joinPlan = logical.Join(
          relationStubToSparkRelation(join.relations(0).left.get).get,
          relationStubToSparkRelation(join.relations(1).left.get).get,
          Inner,
          None)
        if (parentLogicalPlan.isDefined) {
          var newChildren = ArrayBuffer[LogicalPlan]()
          for (i <- parentLogicalPlan.get.children.indices) {
            if (i != translatedChildIndex.get) {
              val c = parentLogicalPlan.get.children(i)
              newChildren += c
            } else {
              newChildren += joinPlan
            }
          }
          parentLogicalPlan.get.withNewChildren(newChildren.toSeq)
          parentLogicalPlan.get
        } else {
          joinPlan
        }
      } else {
        val joinPlan = logical.Join(
          relationStubToSparkRelation(join.relations(0).left.get).get,
          queryInstructionToLogicalPlan(join.relations(1).right.get),
          Inner,
          None)
        if (parentLogicalPlan.isDefined) {
          var newChildren = ArrayBuffer[LogicalPlan]()
          for (i <- parentLogicalPlan.get.children.indices) {
            if (i != translatedChildIndex.get) {
              val c = parentLogicalPlan.get.children(i)
              newChildren += c
            } else {
              newChildren += joinPlan
            }
          }
          parentLogicalPlan.get.withNewChildren(newChildren.toSeq)
          parentLogicalPlan.get
        } else {
          joinPlan
        }
      }
    } else {
      throw new Exception(
        "Currently don't know how to translate any other kind of " +
          "QueryInstruction to a logicalPlan.")
    }
  }
}
