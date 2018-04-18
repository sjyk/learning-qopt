package learning

import opt.{QueryInstruction, RelationStub, Transformation}

import org.apache.spark.sql.SparkSession
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.NDArrayIndex
import org.nd4j.linalg.learning.config.Nesterovs
import org.nd4j.linalg.lossfunctions.LossFunctions

import scala.collection.{JavaConverters, mutable}

class LearningConfig() {
  var sampleDepth : Int = 1
  var numRuns : Int = 1000
  var nEpochs : Int = 2
  var lr : Double = 0.01
  var nPredictions : Int = 25
  var optimizationDepth : Int = 1
  var learnerFeatureMaxWidth : Int = -1
  var nHiddenUnits : Int = 300
  var batchSize : Int = 50

  def fromDict(attrMap : Map[String, Double]) : Unit = {
    sampleDepth = if (attrMap.contains("sampleDepth")) {
      attrMap("sampleDepth").asInstanceOf[Int]
    } else sampleDepth
    numRuns = if (attrMap.contains("numRuns")) attrMap("numRuns").asInstanceOf[Int] else numRuns
    nEpochs = if (attrMap.contains("nEpochs")) {
      attrMap("nEpochs").asInstanceOf[Int]
    } else nEpochs
    lr = if (attrMap.contains("lr")) attrMap("lr") else lr
    nPredictions = if (attrMap.contains("nPredictions")) {
      attrMap("nPredictions").asInstanceOf[Int]
    } else nPredictions
    optimizationDepth = if (attrMap.contains("optimizationDepth")) {
      attrMap("optimizationDepth").asInstanceOf[Int]
    } else optimizationDepth
    learnerFeatureMaxWidth = if (attrMap.contains("learnerFeatureMaxWidth")) {
      attrMap("learnerFeatureMaxWidth").asInstanceOf[Int]
    } else learnerFeatureMaxWidth
    nHiddenUnits = if (attrMap.contains("nHiddenUnits")) {
      attrMap("nHiddenUnits").asInstanceOf[Int]
    } else nHiddenUnits
    batchSize = if (attrMap.contains("batchSize")) {
      attrMap("batchSize").asInstanceOf[Int]
    } else batchSize
  }
}

class Learner() {
  // scalastyle:off println
  var storedModel : Option[MultiLayerNetwork] = None
  var sSession : Option[SparkSession] = None
  var cropWarning = false

  def genFeatureMatrix(transforms : Array[Transformation],
                       config : LearningConfig,
                       trainMode : Boolean = false): INDArray = {
    var nRows = 0
    var content = mutable.ArrayBuilder.make[INDArray]
    val featureBase = BaseFeaturization.getBaseSystemFeaturization
    var tFeatureMaxWidth = transforms(0).featurize(trainMode).shape()(1)
    var maxWidth = featureBase.size + tFeatureMaxWidth

    if (config.learnerFeatureMaxWidth != -1) {
      tFeatureMaxWidth = config.learnerFeatureMaxWidth - featureBase.size
      maxWidth = config.learnerFeatureMaxWidth
      if (transforms(0).featurize(trainMode).shape()(1) > tFeatureMaxWidth && !cropWarning) {
        println("WARNING: This learner is losing information by " +
          "cropping columns of features. Create a learner with a larger max width.")
        cropWarning = true
      }
    }
    for (t <- transforms) {
      var transformMatrix = t.featurize(trainMode)
      if (transformMatrix.columns() > tFeatureMaxWidth) {
        transformMatrix = transformMatrix.get(
          NDArrayIndex.all(),
          NDArrayIndex.interval(0, tFeatureMaxWidth))
      } else if (transformMatrix.columns() < tFeatureMaxWidth) {
        transformMatrix = Nd4j.pad(
          transformMatrix,
          Array(Array(0, 0), Array(0, tFeatureMaxWidth - transformMatrix.columns())),
          Nd4j.PadMode.CONSTANT)
      }
      content += transformMatrix
    }
    Nd4j.vstack(JavaConverters.asJavaCollectionConverter(content.result().toSeq).asJavaCollection)
  }

  def genTrainingRun(initialPlan : QueryInstruction,
                     config: LearningConfig): (INDArray, INDArray) = {
    val planSampler = new Sampler(initialPlan, config.sampleDepth)
    val (sampledTransform, sampledPlan) = planSampler.sample()
    // sample a plan, evaluate the total, cost, and keep a running X_train and y
    val Xtrain = genFeatureMatrix(sampledTransform, config, trainMode = true)
    val planCost = sampledPlan.cost
    val ytrain = Nd4j.valueArrayOf(Xtrain.rows(), 1, sampledPlan.cost)
    (Xtrain, ytrain)
  }

  def genTraining(initialPlan : QueryInstruction, config: LearningConfig): (INDArray, INDArray) = {
    var XContent = mutable.ArrayBuffer[INDArray]()
    var yContent = mutable.ArrayBuffer[INDArray]()
    for (i <- 1 to config.numRuns) {
      val (sampleXtrain, sampleYtrain) = genTrainingRun(initialPlan, config)
      XContent = XContent += sampleXtrain
      yContent = yContent += sampleYtrain
    }
    val Xtrain = Nd4j.vstack(
      JavaConverters.asJavaCollectionConverter(XContent).asJavaCollection)
    val yTrain = Nd4j.vstack(JavaConverters.asJavaCollectionConverter(yContent).asJavaCollection)
    println(s"X train dimensions: (${Xtrain.rows()}, ${Xtrain.columns()})")
    (Xtrain, yTrain)
  }

  def buildModel(initialPlan : QueryInstruction, config: LearningConfig) : Boolean = {
    val nnConf = new NeuralNetConfiguration.Builder()
      .seed(12345)
      .weightInit(WeightInit.XAVIER)
      .updater(new Nesterovs(config.lr, 0.9))
      .list
      .layer(0, new DenseLayer.Builder()
        .nIn(config.learnerFeatureMaxWidth)
        .nOut(config.nHiddenUnits)
        .activation(Activation.TANH)
        .build)
      .layer(1, new DenseLayer.Builder()
        .nIn(config.nHiddenUnits)
        .nOut(config.nHiddenUnits)
        .activation(Activation.TANH)
        .build)
      .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
        .activation(Activation.IDENTITY)
        .nIn(config.nHiddenUnits)
        .nOut(1)
        .build)
      .pretrain(false)
      .backprop(true)
      .build
    val net = new MultiLayerNetwork(nnConf)
    println("Generating training data")
    val (trainData, trainLabels) = genTraining(initialPlan, config)
    println("Generated training data. Preparing and parallelizing.")
    val dSet = new DataSet(trainData, trainLabels)
    var dList = dSet.asList()
    dList = scala.util.Random.shuffle(dList)
    val dIter = new ListDataSetIterator(dList, config.batchSize)
    println("Fitting the model")
    net.init()
    for (i <- 0 to config.nEpochs) {
      dIter.reset()
      net.fit(dIter)
    }
    dIter.reset()
    val preds = net.output(dIter)
    // evaluate success
    val MSE = preds.squaredDistance(trainLabels)
    println(s"Training Mean Squared Error = $MSE")
    println("Finished fitting the model.")
    storedModel = Some(net)
    true
  }

  def predict(plan : QueryInstruction,
              config : LearningConfig = new LearningConfig())
  : (Array[QueryInstruction], Array[Double]) = {
    if (storedModel.isEmpty) {
      val modelBuilt = buildModel(plan, config)
      if (!modelBuilt) {
        throw new Exception("Model failed to build.")
      }
    }
    val pSampler = new Sampler(plan, 1, false)
    val (transforms, instructions) = pSampler.sampleN(config.nPredictions)
    val featurizedTransforms = transforms.map(t => genFeatureMatrix(t, config))
    val tMatrix = Nd4j.vstack(
      JavaConverters.asJavaCollectionConverter(featurizedTransforms).asJavaCollection)
    println(s"X test dimensions: (${tMatrix.rows()}, ${tMatrix.columns()})")
    val predictions = storedModel.get.predict(tMatrix).toArray[Double]
    println("Finished predict loop")
    (instructions, predictions)
  }

  /* Predict and find the best value plan */
  def optimizeAndExecute(plan : QueryInstruction, config : LearningConfig = new LearningConfig())
  : (QueryInstruction, RelationStub) = {
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
    println(s"best plan: $bestPlan")
    println(s"best plan had cost: ${bestPlan.cost}")
    println(s"solution relation had cost: ${solution.initCost}")
    (bestPlan, solution)
  }
  // scalastyle:on println
}
