name := "ALDataLab"

version := "1.3.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.2"

enablePlugins(UniversalPlugin)

mappings in Universal <+= (assembly in assembly, assemblyJarName in assembly) map { (jar,name) => {
  println(jar.toString)
  jar -> ("lib/" + name)
}}

import S3._

s3Settings

host in upload := "gedatalab.s3.amazonaws.com"

mappings in upload <+= (packageBin in Universal, packageName in Universal) map {
  (bin,name) => (bin,"binaries/"+name)
}
