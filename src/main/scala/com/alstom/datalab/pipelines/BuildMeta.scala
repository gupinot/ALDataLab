package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import com.alstom.datalab.Util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by raphael on 02/12/2015.
  */
class BuildMeta(sqlContext: SQLContext) extends Pipeline {

  import sqlContext.implicits._

  override def execute(): Unit = {
    // try to read the dataframes metadata
    val meta = List("connection","webrequest").map( filetype => {
      try {
        sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/$filetype/")
          .select(lit("connections") as "filetype", lit(Pipeline2To3.STAGE_NAME) as "stage", $"collecttype",$"engine",$"dt".cast("string").as("dt"),$"filedt")
          .distinct()
      } catch {
        case _:Throwable => sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row],StructType(List(
            StructField("filetype", StringType, true),
            StructField("stage", StringType, true),
            StructField("collecttype", StringType, true),
            StructField("engine", StringType, true),
            StructField("dt", StringType, true),
            StructField("filedt", StringType, true)
          )))
      }
    })
    // now merge all found dataframes and save it
    meta.reduce(_.unionAll(_)).repartition(1)
      .write.mode(SaveMode.Overwrite)
      .parquet(context.dirout())
  }
}
