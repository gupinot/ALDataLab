package com.alstom.datalab

import com.alstom.datalab.pipelines.{Pipeline3To4, Pipeline4To5, Pipeline2To3}
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

  def deltaMeta3to4(path: String)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val stage1 = Pipeline2To3.STAGE_NAME
    val stage2 = Pipeline3To4.STAGE_NAME
    val metaDf1 = aggregateMeta(loadMeta(path), stage1).as("metaDf1")
    val metaDf2 = aggregateMeta(loadMeta(path), stage2).as("metaDf2")

    //keep only connection delta (meta23 not in meta34)
    val cnx_meta_delta = metaDf1.filter($"metaDf1.filetype" === "connection").join(metaDf2,
      ($"metaDf1.dt" === $"metaDf2.dt") and ($"metaDf1.engine" === $"metaDf2.engine")
        and ($"metaDf1.collecttype" === $"metaDf2.collecttype") and ($"metaDf1.filetype" === $"metaDf2.filetype")
        and ($"metaDf1.min_filedt" === $"metaDf2.min_filedt"),
      "left_outer")
      .filter("metaDf2.dt is null")
      .select(metaDf1.columns.map(metaDf1.col):_*).as("cnx_meta_delta")

    //select only record from cnx_meta_delta with corresponding webrequest record in metaDf1
    cnx_meta_delta.join(
      metaDf1.filter($"filetype" === "webrequest").select("engine","min_filedt").distinct().as("metaDf1"),
      ($"cnx_meta_delta.engine" === $"metaDf1.engine")
        and ($"cnx_meta_delta.min_filedt" === $"metaDf1.min_filedt"),
      "inner")
      .select(cnx_meta_delta.columns.map(cnx_meta_delta.col):_*)
  }

  def deltaMeta4to5(path: String)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val stage1 = Pipeline3To4.STAGE_NAME
    val stage2 = Pipeline4To5.STAGE_NAME
    val metaDf1 = aggregateMeta(loadMeta(path), stage1).as("metaDf1")
    val metaDf2 = aggregateMeta(loadMeta(path), stage2).as("metaDf2")

    //keep only connection delta (meta23 not in meta34)
    metaDf1.join(metaDf2,
      ($"metaDf1.dt" === $"metaDf2.dt") and ($"metaDf1.engine" === $"metaDf2.engine")
        and ($"metaDf1.collecttype" === $"metaDf2.collecttype") and ($"metaDf1.filetype" === $"metaDf2.filetype")
        and ($"metaDf1.min_filedt" === $"metaDf2.min_filedt"),
      "left_outer")
      .filter("metaDf2.dt is null")
      .select(metaDf1.columns.map(metaDf1.col):_*).as("cnx_meta_delta")

  }
}
