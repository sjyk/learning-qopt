name := "Learning Query Optimization"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++=  Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-hive" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.spark" %% "spark-streaming-flume" % "2.2.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0")

classpathTypes += "maven-plugin"

libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "1.0.0-alpha"

libraryDependencies += "org.bytedeco" % "javacpp" % "1.4.1"

libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-alpha"