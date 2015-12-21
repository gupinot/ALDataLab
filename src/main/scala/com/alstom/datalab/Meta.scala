package com.alstom.datalab

import com.alstom.datalab.pipelines.Pipeline2To3
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by raphael on 14/12/2015.
  */
trait Meta {

  def loadMeta(path: String)(implicit sqlContext: SQLContext) = try {
    sqlContext.read.parquet(path)
  } catch {
    case e:Throwable =>
      sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row],StructType(List(
        StructField("filetype", StringType, true),
        StructField("stage", StringType, true),
        StructField("collecttype", StringType, true),
        StructField("engine", StringType, true),
        StructField("dt", StringType, true),
        StructField("filedt", StringType, true)
      )))
  }

  def aggregateMeta(metaDf: DataFrame, stage :String)(implicit sqlContext: SQLContext) = {
  import sqlContext.implicits._
    metaDf
    .filter($"stage" === stage)
    .groupBy("collecttype","engine","dt","filetype")
    .agg(min($"filedt").as("min_filedt"))
    .withColumn("dt",to_date($"dt"))
  }
}
