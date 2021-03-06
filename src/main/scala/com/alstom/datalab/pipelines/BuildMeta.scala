package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import com.alstom.datalab.Util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, DateType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by raphael on 02/12/2015.
  */
class BuildMeta(sqlContext: SQLContext) extends Pipeline {

  import sqlContext.implicits._

  override def execute(): Unit = {
    // try to read the dataframes metadata
    val meta = List("connection", "webrequest", "execution").map(filetype => {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/$filetype/")
          .select(lit(filetype) as "filetype", lit(Pipeline2To3.STAGE_NAME) as "stage", $"collecttype", $"engine", to_date($"dt") as "dt", $"filedt")
          .distinct()
      } catch {
        case _: Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], StructType(List(
          StructField("filetype", StringType, true),
          StructField("stage", StringType, true),
          StructField("collecttype", StringType, true),
          StructField("engine", StringType, true),
          StructField("dt", DateType, true),
          StructField("filedt", StringType, true)
        )))
      }
    }).reduce(_.unionAll(_))

    val meta2 = {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirout()}")
          .select(lit("connection") as "filetype", lit(Pipeline3To4.STAGE_NAME) as "stage", $"collecttype", $"engine", to_date($"dt") as "dt", $"filedt")
          .distinct()

      }
      catch {
        case _: Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], StructType(List(
          StructField("filetype", StringType, true),
          StructField("stage", StringType, true),
          StructField("collecttype", StringType, true),
          StructField("engine", StringType, true),
          StructField("dt", DateType, true),
          StructField("filedt", StringType, true)
        )))
      }
    }.unionAll(meta)

    val meta2bis = {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirout()}_execution")
          .select(lit("execution") as "filetype", lit(Pipeline3To4.STAGE_NAME) as "stage", $"collecttype", $"engine", to_date($"dt") as "dt", $"filedt")
          .distinct()

      }
      catch {
        case _: Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], StructType(List(
          StructField("filetype", StringType, true),
          StructField("stage", StringType, true),
          StructField("collecttype", StringType, true),
          StructField("engine", StringType, true),
          StructField("dt", DateType, true),
          StructField("filedt", StringType, true)
        )))
      }
    }.unionAll(meta2)

    val meta3 = {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.diragg()}/device")
          .select(lit("connection") as "filetype", lit(Pipeline4To5.STAGE_NAME) as "stage", lit("device") as "collecttype", $"engine", to_date($"dt") as "dt", $"filedt")
          .distinct()
      }
      catch {
        case _: Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], StructType(List(
          StructField("filetype", StringType, true),
          StructField("stage", StringType, true),
          StructField("collecttype", StringType, true),
          StructField("engine", StringType, true),
          StructField("dt", DateType, true),
          StructField("filedt", StringType, true)
        )))
      }
    }.unionAll(meta2bis)

    val meta4 = {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.diragg()}/server")
          .select(lit("connection") as "filetype", lit(Pipeline4To5.STAGE_NAME) as "stage", lit("server") as "collecttype", $"engine", to_date($"dt") as "dt", $"filedt")
          .distinct()
      }
      catch {
        case _: Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], StructType(List(
          StructField("filetype", StringType, true),
          StructField("stage", StringType, true),
          StructField("collecttype", StringType, true),
          StructField("engine", StringType, true),
          StructField("dt", DateType, true),
          StructField("filedt", StringType, true)
        )))
      }
    }.unionAll(meta3)

    // now merge all found dataframes and save it
    meta4.repartition(1)
      .write.mode(SaveMode.Overwrite)
      .parquet(context.meta())
  }
}
