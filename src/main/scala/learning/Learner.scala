package learning

import opt.{QueryInstruction, RelationStub, Transformation}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class LearningConfig() {
  var sampleDepth : Int = 1
  var numRuns : Int = 1000
  var numTrainingIterations : Int = 4000
  var lr : Double = 0.01
  var nPredictions : Int = 25
  var optimizationDepth : Int = 1
  var learnerFeatureMaxWidth : Int = -1

  def fromDict(attrMap : Map[String, Double]) : Unit = {
    sampleDepth = if (attrMap.contains("sampleDepth")) attrMap("sampleDepth").asInstanceOf[Int] else sampleDepth
    numRuns = if (attrMap.contains("numRuns")) attrMap("numRuns").asInstanceOf[Int] else numRuns
    numTrainingIterations =
      if (attrMap.contains("numTrainingIterations"))
        attrMap("numTrainingIterations").asInstanceOf[Int]
      else numTrainingIterations
    lr = if (attrMap.contains("lr")) attrMap("lr") else lr
    nPredictions = if (attrMap.contains("nPredictions")) attrMap("nPredictions").asInstanceOf[Int] else nPredictions
    optimizationDepth =
      if (attrMap.contains("optimizationDepth"))
        attrMap("optimizationDepth").asInstanceOf[Int]
      else optimizationDepth
  }
}

class Learner() {

  var storedModel : Option[LinearRegressionModel] = None
  var sSession : Option[SparkSession] = None
  var cropWarning = false

  def genFeatureMatrix(transforms : Array[Transformation], config : LearningConfig, trainMode : Boolean = false): Matrix = {
    var nRows = 0
    var content = ArrayBuffer[Double]()
    val featureBase = BaseFeaturization.getBaseSystemFeaturization
    var tFeatureMaxWidth = transforms(0).featurize(trainMode).numCols
    var maxWidth = featureBase.size + tFeatureMaxWidth

    if (config.learnerFeatureMaxWidth != -1) {
      tFeatureMaxWidth = config.learnerFeatureMaxWidth - featureBase.size
      maxWidth = config.learnerFeatureMaxWidth
      if (transforms(0).featurize(trainMode).numCols > tFeatureMaxWidth && !cropWarning) {
        println("WARNING: This learner is losing information by cropping columns of features. Create a learner with a larger max width.")
        cropWarning = true
      }
    }

    for (t <- transforms) {
      val transformMatrix = t.featurize(trainMode)
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
    /* Column major matrix */
    Matrices.dense(maxWidth, nRows, content.toArray).transpose
  }

  def genTrainingRun(initialPlan : QueryInstruction, config: LearningConfig): (Matrix, Vector) = {
    val planSampler = new Sampler(initialPlan, config.sampleDepth)
    val (sampledTransform, sampledPlan) = planSampler.sample()
    // sample a plan, evaluate the total, cost, and keep a running X_train and y
    val Xtrain = genFeatureMatrix(sampledTransform, config, trainMode = true)
    val planCost = sampledPlan.cost
    val ytrain = Vectors.dense(Array.fill[Double](Xtrain.numRows){planCost})
    (Xtrain, ytrain)
  }

  def genTraining(initialPlan : QueryInstruction, config: LearningConfig): (Matrix, Vector) = {
    var XContent = ArrayBuffer[Matrix]()
    var yContent = Array[Double]()
    for (i <- 1 to config.numRuns) {
      val (sampleXtrain, sampleYtrain) = genTrainingRun(initialPlan, config)
      XContent = XContent += sampleXtrain
      yContent = yContent ++ sampleYtrain.toArray
    }
    val Xtrain = Matrices.vertcat(XContent.toArray)
    println(s"X train dimensions: (${Xtrain.numRows}, ${Xtrain.numCols})")
    (Xtrain, Vectors.dense(yContent))
  }

  def buildModel(initialPlan : QueryInstruction, config: LearningConfig) : Boolean = {
    val spark = SparkSession
      .builder
      .appName("Learning Query Optimizer")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sSession = Some(spark)
    println("Generating training data")
    val (trainData, trainLabels) = genTraining(initialPlan, config)
    println("Generated training data. Preparing and parallelizing.")
    val dfPrep = trainData.rowIter.toSeq.zipWithIndex.map(x => LabeledPoint(trainLabels(x._2), x._1))
    val training = spark.sparkContext.parallelize(dfPrep)
    println("Fitting the model")
    val m = LinearRegressionWithSGD.train(training, config.numTrainingIterations, config.lr)
    val valuesAndPreds = training.map { point =>
      val prediction = m.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow(v - p, 2)}.mean()
    println(s"Training Mean Squared Error = ${MSE}")
    println("Finished fitting the model.")
    storedModel = Some(m)
    true
  }

  def predict(plan : QueryInstruction, config : LearningConfig = new LearningConfig()) : (Array[QueryInstruction], Array[Double]) = {
    if (storedModel.isEmpty) {
      val modelBuilt = buildModel(plan, config)
      if (!modelBuilt) {
        throw new Exception("Model failed to build.")
      }
    }
    val pSampler = new Sampler(plan, 1, false)
    val (transforms, instructions) = pSampler.sampleN(config.nPredictions)
    val featurizedTransforms = transforms.map(t => genFeatureMatrix(t, config))
    val tMatrix = Matrices.vertcat(featurizedTransforms)
    println(s"X test dimensions: (${tMatrix.numRows}, ${tMatrix.numCols})")
    val dfPrep = tMatrix.rowIter.toSeq
    val training = sSession.get.sparkContext.parallelize(dfPrep)
    val predictions = storedModel.get.predict(training)
    println("Finished predict loop")
    (instructions, predictions.collect())
  }

  /* Predict and find the best value plan */
  def optimizeAndExecute(plan : QueryInstruction, config : LearningConfig = new LearningConfig()) : RelationStub = {
    var bestPlan = plan
    for (i <- 1 to config.optimizationDepth) {
      val (instructions, preds) = predict(bestPlan, config)
      var minIdx = -1
      var min = scala.Double.MaxValue
      for (i <- preds.indices) {
        if (preds(i) < min) {
          min = preds(i)
          minIdx = i
        }
      }
      bestPlan = instructions(minIdx)
    }
    val spark = sSession.get
    spark.stop()
    val solution = bestPlan.execute
    println(s"best plan: ${bestPlan}")
    println(s"best plan had cost: ${bestPlan.cost}")
    println(s"solution relation had cost: ${solution.initCost}")
    solution
  }
}
