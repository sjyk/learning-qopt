package learning

import opt.{QueryInstruction, RelationStub, Transformation}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class Learner(maxWidth : Int) {

  var storedModel : Option[LinearRegressionModel] = None
  var sSession : Option[SparkSession] = None

  def genFeatureMatrix(transforms : Array[Transformation]): Matrix = {
    var nRows = 0
    var content = ArrayBuffer[Double]()
    val featureBase = BaseFeaturization.getBaseSystemFeaturization
    val tFeatureMaxWidth = maxWidth - featureBase.size
    for (t <- transforms) {
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
        content = content ++ (featureBase ++ nRow.get.to[ArrayBuffer])
        nRows += 1
      }
    }
    Matrices.dense(nRows, maxWidth, content.toArray)
  }

  def genTrainingRun(initialPlan : QueryInstruction): (Matrix, Vector) = {
    val planSampler = new Sampler(initialPlan, 5)
    val (sampledTransform, sampledPlan) = planSampler.sample()
    // sample a plan, evaluate the total, cost, and keep a running X_train and y
    val Xtrain = genFeatureMatrix(sampledTransform)
    val planCost = sampledPlan.cost
    println(s"Plan cost ${planCost}")
    val ytrain = Vectors.dense(Array.fill[Double](Xtrain.numRows){planCost})
    (Xtrain, ytrain)
  }

  def genTraining(initialPlan : QueryInstruction, maxIter : Int = 500): (Matrix, Vector) = {
    var XContent = ArrayBuffer[Matrix]()
    var yContent = Array[Double]()
    for (i <- 1 to maxIter) {
      val (sampleXtrain, sampleYtrain) = genTrainingRun(initialPlan)
      XContent = XContent += sampleXtrain
      yContent = yContent ++ sampleYtrain.toArray
    }
    val Xtrain = Matrices.vertcat(XContent.toArray)
    println(s"X train dimensions: (${Xtrain.numRows}, ${Xtrain.numCols})")
    (Xtrain, Vectors.dense(yContent))
  }

  def buildModel(initialPlan : QueryInstruction) : Boolean = {
    val spark = SparkSession
      .builder
      .appName("Learning Query Optimizer")
      .config("spark.master", "local")
      .getOrCreate()
    sSession = Some(spark)
    println("Generating training data")
    val (trainData, trainLabels) = genTraining(initialPlan)
    val dfPrep = trainData.rowIter.toSeq.zipWithIndex.map(x => LabeledPoint(trainLabels(x._2), x._1))
    val training = spark.sparkContext.parallelize(dfPrep)
    println("Fitting the model")
    val m = new LinearRegressionWithSGD().setValidateData(false)
    val model = m.run(training)
    // Summarize the model over the training set and print out some metrics
    // Print the coefficients and intercept for linear regression
    println("Finished fitting the model.")
    storedModel = Some(model)
    true
  }

  def predict(plan : QueryInstruction, tDepth : Int = 1) : (Array[QueryInstruction], Array[Double]) = {
    if (storedModel.isEmpty) {
      val modelBuilt = buildModel(plan)
      if (!modelBuilt) {
        throw new Exception("Model failed to build.")
      }
    }
    val pSampler = new Sampler(plan, tDepth, false)
    val (transforms, instructions) = pSampler.sampleN(50)
    val featurizedTransforms = transforms.map(t => genFeatureMatrix(t))
    val tMatrix = Matrices.vertcat(featurizedTransforms)
    val dfPrep = tMatrix.rowIter.toSeq
    val training = sSession.get.sparkContext.parallelize(dfPrep)
    val predictions = storedModel.get.predict(training)
    println(predictions.collect().mkString("\n"))
    println("Finished predict loop")
    (instructions, predictions.collect())
  }

  /* Predict and find the best value plan */
  def optimizeAndExecute(plan : QueryInstruction) : RelationStub = {
    val (instructions, preds) = predict(plan)
    var minIdx = -1
    var min = scala.Double.MaxValue
    for (i <- preds.indices) {
      if (preds(i) < min) {
        min = preds(i)
        minIdx = i
      }
    }
    val bestPlan = instructions(minIdx)
    val spark = sSession.get
    spark.stop()
    val solution = bestPlan.execute
    println(s"best plan had cost: ${solution.initCost}")
    solution
  }
}
