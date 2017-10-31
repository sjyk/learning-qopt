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
