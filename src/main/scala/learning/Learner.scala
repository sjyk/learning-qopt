package learning

import opt.{QueryInstruction, RelationStub, Transformation}
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, GeneralizedLinearRegressionModel, LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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
    val sampledTransform = planSampler.sample()._1
    // sample a plan, evaluate the total, cost, and keep a running X_train and y
    val Xtrain = genFeatureMatrix(sampledTransform)
    val planCost = initialPlan.cost
    val ytrain = Vectors.dense(Array.fill[Double](Xtrain.numRows){planCost})
    (Xtrain, ytrain)
  }

  def genTraining(initialPlan : QueryInstruction, maxIter : Int = 100): (Matrix, Vector) = {
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
    val dfPrep = trainData.rowIter.toSeq.zipWithIndex.map(x => (x._1, trainLabels(x._2)))
    val training = spark.createDataFrame(dfPrep).toDF("features", "cost")
    val glr = new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setLabelCol("cost")
      .setFeaturesCol("features")
    println("Fitting the model")
    val model = glr.fit(training)
    // Summarize the model over the training set and print out some metrics
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")
    println("Finished fitting the model")
    storedModel = Some(model)
    true
  }

  def predict(plan : QueryInstruction, tDepth : Int = 1) : DataFrame = {
    if (storedModel.isEmpty) {
      val modelBuilt = buildModel(plan)
      if (!modelBuilt) {
        throw new Exception("Model failed to build.")
      }
    }
    val pSampler = new Sampler(plan, tDepth, false)
    val (transforms, instructions) = pSampler.sampleN(50)
    println(transforms.deep.mkString("\n"))
    println(instructions.mkString("\n"))
    val featurizedTransforms = transforms.map(t => genFeatureMatrix(t))
    val tMatrix = Matrices.vertcat(featurizedTransforms).rowIter.toSeq.map(x=>(x, 0))
    val inputDF = sSession.get.createDataFrame(tMatrix).toDF("features", "cost")
    val predictions = storedModel.get.transform(inputDF)
    predictions.select("prediction", "cost").show(50)
    println("Finished predict loop")
    predictions
  }

  def optimizeAndExecute(plan : QueryInstruction) : RelationStub = {
    // predict,
    var result = predict(plan)
    println(result.rdd.toDebugString)
    val spark = sSession.get
    spark.stop()
    plan.execute
  }
}
