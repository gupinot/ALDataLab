name := "ALDataLab"

version := "1.0"

scalaVersion := "2.10.4"

//libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value
libraryDependencies +="org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies +="org.apache.spark" %% "spark-sql" % "1.5.1" % "provided"
libraryDependencies +="org.apache.hadoop" % "hadoop-client" % "2.7.1" % "provided"
libraryDependencies +="com.databricks" %% "spark-csv" % "1.2.0"
libraryDependencies +="joda-time" % "joda-time" % "2.8"

