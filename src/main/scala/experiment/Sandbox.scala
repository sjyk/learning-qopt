package experiment

import dml.JoinUtils
import learning.Learner
import opt.RelationStub

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Sandbox {
  def main(args: Array[String]) : Unit = {
    val r1 = new RelationStub(
      "a",
      mutable.Set(Seq("1"), Seq("2"), Seq("4"), Seq("5"), Seq("6"), Seq("7")))
    val r2 = new RelationStub(
      "b",
      mutable.Set(Seq("1"), Seq("2"), Seq("3"), Seq("5"), Seq("6"), Seq("10")))
    val r3 = new RelationStub("c", mutable.Set(Seq("1"), Seq("3"), Seq("4"), Seq("5")))
    val r4 = new RelationStub("d", mutable.Set(Seq("4"), Seq("2"), Seq("5"), Seq("6")))
    val r5 = new RelationStub("e", mutable.Set(Seq("5"), Seq("4"), Seq("3"), Seq("8")))
    val r6 = new RelationStub("f", mutable.Set(Seq("4"), Seq("2"), Seq("3"), Seq("6"), Seq("10")))
    val i = r6.relationContent & r5.relationContent
    val relations = ArrayBuffer[RelationStub](r1, r2, r3, r4, r5, r6)
    val join = JoinUtils.initJoinFromList(relations)
    val learner = new Learner()
    learner.optimizeAndExecute(join)
  }
}
