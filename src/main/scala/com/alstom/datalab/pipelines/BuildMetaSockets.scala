package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by Guillaume on 02/02/2016.
  */
class BuildMetaSockets(sqlContext: SQLContext) extends Pipeline {

  import sqlContext.implicits._

  override def execute(): Unit = {
    // try to read the dataframes metadata
    val meta = List("ps", "lsof", "netstat").map(filetype => {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/$filetype/")
          .select(lit(filetype) as "filetype", lit(EncodeServerSockets.STAGE_NAME) as "stage", $"server_ip", $"dt")
          .distinct()
      } catch {
        case _: Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], StructType(List(
          StructField("filetype", StringType, true),
          StructField("stage", StringType, true),
          StructField("server_ip", StringType, true),
          StructField("dt", StringType, true)
        )))
      }
    }).reduce(_.unionAll(_))

    val meta2 = {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirout()}")
          .select(lit("lsof") as "filetype", lit(ResolveServerSockets.STAGE_NAME) as "stage", $"engine" as "server_ip", $"dt")
          .distinct()
      }
      catch {
        case _: Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], StructType(List(
          StructField("filetype", StringType, true),
          StructField("stage", StringType, true),
          StructField("server_ip", StringType, true),
          StructField("dt", StringType, true)
        )))
      }
    }.unionAll(meta)


    val meta3 = {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.diragg()}")
          .select(lit("lsof") as "filetype", lit(AggregateServerSockets.STAGE_NAME) as "stage", $"engine" as "server_ip", $"dt")
          .distinct()
      }
      catch {
        case _: Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], StructType(List(
          StructField("filetype", StringType, true),
          StructField("stage", StringType, true),
          StructField("server_ip", StringType, true),
          StructField("dt", StringType, true)
        )))
      }
    }.unionAll(meta)

    // now merge all found dataframes and save it
    meta3.repartition(1)
      .write.mode(SaveMode.Overwrite)
      .parquet(context.meta())
  }
}
