package experiment

import dml._
import learning.{Learner, LearningConfig}
import opt.ConstraintStub

import scala.collection.mutable.ArrayBuffer

object FindReplaceExample {
  def main(args: Array[String]) : Unit = {
    val r1 = new DataRelationStub("r1", Array(
      Array("0", "1", "2", "3", "4", "5"),
      Array("a", "a", "a", "a", "a", "a"),
      Array("b", "b", "b", "b", "b", "b"),
      Array("c", "c", "c", "c", "c", "c"),
      Array("d", "d", "d", "d", "d", "d"),
      Array("e", "e", "e", "e", "e", "e"),
      Array("f", "f", "f", "f", "f", "f")))
    val r2 = new DataRelationStub("r2", Array(
      Array("0", "1", "2", "3", "4", "5"),
      Array("a", "a", "a", "a", "a", "a"),
      Array("b", "b", "b", "b", "b", "b"),
      Array("c", "c", "c", "c", "c", "c")))
    val c1 = ArrayBuffer(
      new ConstraintStub(ArrayBuffer[Either[Int, String]](Right("a"), Right("replaceda1"), Left(1))),
      new ConstraintStub(ArrayBuffer[Either[Int, String]](Right("a"), Right("replaceda3"), Left(3))))
    val c2 = ArrayBuffer(
      new ConstraintStub(ArrayBuffer[Either[Int, String]](Right("a"), Right("replaceda1"), Left(1))),
      new ConstraintStub(ArrayBuffer[Either[Int, String]](Right("a"), Right("replaceda3"), Left(3))))
    val c3 = ArrayBuffer(
      new ConstraintStub(ArrayBuffer[Either[Int, String]](Right("c"), Right("replacedc1"), Left(1))),
      new ConstraintStub(ArrayBuffer[Either[Int, String]](Right("a"), Right("replaceda4"), Left(4))))
    val find = DCUtils.initFindFromList(ArrayBuffer(Left(r1), Right(c1), Left(r2), Right(c2), Left(r1), Right(c3)))
    println(find)
    println(s"Initial find has cost: ${find.cost}")
    println("Optimal cost for this plan is 29.")
    val config = new LearningConfig()
    config.fromDict(
      Map[String, Double](
        ("sampleDepth", 1),
        ("lr", 0.01),
        ("numTrainingIterations", 4000),
        ("optimizationDepth", 1)))
    val learner = new Learner()
    learner.optimizeAndExecute(find, config)
  }
}
