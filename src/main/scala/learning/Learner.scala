package learning

import opt.QueryInstruction
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, GeneralizedLinearRegressionModel}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class Learner(maxWidth : Int) {

  var storedModel : Option[GeneralizedLinearRegressionModel] = None

  def genTrainingRun(initialPlan : QueryInstruction, maxWidth : Int): (Matrix, Vector) = {
    val planSampler = new Sampler(initialPlan, 5)
    val sampledTransform = planSampler.sample()._1
    var nRows = 0
    var content = ArrayBuffer[Double]()
    val featureBase = BaseFeaturization.getBaseSystemFeaturization
    val tFeatureMaxWidth = maxWidth - featureBase.size
    for (t <- sampledTransform) {
      val transformMatrix = t.featurize
      for (row <- transformMatrix.rowIter) {
        var nRow : Option[Array[Double]] = None
        if (row.size > tFeatureMaxWidth) {
          nRow = Some(row.toArray.take(tFeatureMaxWidth).toVector.toArray)
        } else if (row.size < tFeatureMaxWidth) {
          nRow = Some(row.toArray ++ Array.fill[Double](tFeatureMaxWidth - row.size)(0))
        } else {
          nRow = Some(row.toArray)
        }
        content = content ++ (featureBase ++ nRow).asInstanceOf[ArrayBuffer[Double]]
        nRows += 1
      }
    }
    // sample a plan, evaluate the total, cost, and keep a running X_train and y
    val Xtrain = Matrices.dense(nRows, maxWidth, content.toArray)
    val planCost = initialPlan.cost
    val ytrain = Vectors.dense(Array.fill[Double](nRows){planCost})
    (Xtrain, ytrain)
  }

  def genTraining(initialPlan : QueryInstruction, maxIter : Int = 1000, maxWidth : Int = 200): (Matrix, Vector) = {
    var XContent = ArrayBuffer[Matrix]()
    var yContent = Array[Double]()
    for (i <- 1 to maxIter) {
      val (sampleXtrain, sampleYtrain) = genTrainingRun(initialPlan, maxWidth)
      XContent = XContent += sampleXtrain
      yContent = yContent ++ sampleYtrain.toArray
    }
    val Xtrain = Matrices.vertcat(XContent.toArray)
    (Xtrain, Vectors.dense(yContent))
  }

  def buildModel(initialPlan : QueryInstruction) : Boolean = {
    val spark = SparkSession
      .builder
      .appName("Learning Query Optimizer")
      .getOrCreate()_
    val (trainData, trainLabels) = genTraining(initialPlan)
    val dfPrep = trainData.rowIter.toSeq.zipWithIndex.map(x => (x._1, trainLabels(x._2)))
    val training = spark.createDataFrame(dfPrep).toDF("features", "cost")
    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("log")
      .setRegParam(0.3)
      .setLabelCol("cost")
      .setFeaturesCol("features")
    val model = glr.fit(training)
    storedModel = Some(model)
    true
  }
}
