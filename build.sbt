// sbt build file.
// See: http://www.scala-sbt.org/0.13/docs/index.html

name := "Knapsack"

scalaVersion := "2.11.6"
sparkVersion := "2.0.0"
version := "1.0"
spAppendScalaVersion := true


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.11" % "2.0.0" % "provided")

// change the value below to change the directory where your zip artifact will be created
spDistDirectory := target.value
licenses := Seq("Apache-2.0" -> url("https://github.com/drulm/Spark_Knapsack/blob/master/LICENSE"))
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials4")
sparkComponents += "core"

// add any sparkPackageDependencies using sparkPackageDependencies.
// e.g. sparkPackageDependencies += "databricks/spark-avro:0.1"
sparkPackageName := "drulm/Spark_Knapsack"




