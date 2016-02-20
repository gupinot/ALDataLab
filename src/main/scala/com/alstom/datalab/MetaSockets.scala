package com.alstom.datalab

import com.alstom.datalab.pipelines._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by raphael on 14/12/2015.
  */
trait MetaSockets {

  def loadMetaSockets(path: String)(implicit sqlContext: SQLContext) = try {
    sqlContext.read.parquet(path)
  } catch {
    case e:Throwable =>
      sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row],StructType(List(
        StructField("filetype", StringType, true),
        StructField("stage", StringType, true),
        StructField("server_ip", StringType, true),
        StructField("dt", StringType, true)
      )))
  }

  def aggregateMetaSockets(metaDf: DataFrame, stage :String)(implicit sqlContext: SQLContext) = {
  import sqlContext.implicits._
    metaDf
    .filter($"stage" === stage)
    .select("filetype","server_ip","dt")
    .distinct()
  }

  def deltaMetaEncodedToResolved(path: String)(implicit sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val stage1 = EncodeServerSockets.STAGE_NAME
    val stage2 = ResolveServerSockets.STAGE_NAME
    val metaDf1 = aggregateMetaSockets(loadMetaSockets(path), stage1).as("metaDf1")
    val metaDf2 = aggregateMetaSockets(loadMetaSockets(path), stage2).as("metaDf2")

    //keep only lsof delta
    metaDf1.filter($"metaDf1.filetype" === "lsof").join(metaDf2,
      ($"metaDf1.dt" === $"metaDf2.dt") and ($"metaDf1.server_ip" === $"metaDf2.server_ip")
        and ($"metaDf1.filetype" === $"metaDf2.filetype"),
      "left_outer")
      .filter("metaDf2.dt is null")
      .select(metaDf1.columns.map(metaDf1.col):_*)
  }
}
