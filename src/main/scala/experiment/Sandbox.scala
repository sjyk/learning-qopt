package experiment

import dml.{Join, JoinUtils}
import learning.Learner

import scala.collection.mutable.{ArrayBuffer, Set}
import opt.{ConstraintStub, QueryInstruction, RelationStub}

object Sandbox {
  def main(args: Array[String]) : Unit = {
    val r1 = new RelationStub("a", Set(Array("1"), Array("2"), Array("4"), Array("5"), Array("6"), Array("7")))
    val r2 = new RelationStub("b", Set(Array("1"), Array("2"), Array("3"), Array("5"), Array("6"), Array("10")))
    val r3 = new RelationStub("c", Set(Array("1"), Array("3"), Array("4"), Array("5")))
    val r4 = new RelationStub("d", Set(Array("4"), Array("2"), Array("5"), Array("6")))
    val r5 = new RelationStub("e", Set(Array("5"), Array("4"), Array("3"), Array("8")))
    val r6 = new RelationStub("f", Set(Array("4"), Array("2"), Array("3"), Array("6"), Array("10")))
    val relations = ArrayBuffer[RelationStub](r1, r2, r3, r4, r5, r6)
    val find = JoinUtils.initJoinFromList(relations)
    println(find)
    val learner = new Learner(15)
    learner.optimizeAndExecute(find)
  }
}
