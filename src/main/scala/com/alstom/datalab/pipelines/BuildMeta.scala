package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import com.alstom.datalab.Util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by raphael on 02/12/2015.
  */
class BuildMeta(sqlContext: SQLContext) extends Pipeline {

  import sqlContext.implicits._

  override def execute(): Unit = {
    // main dataframes
    val cnx = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/connection/")
    val wr = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/webrequest/")

    // metadata view on main dataframes
    cnx.select(lit("connections") as "filetype", lit(Pipeline2To3.STAGE_NAME) as "stage", $"collecttype",$"engine",$"dt",$"filedt")
      .distinct()
    .unionAll(wr.select(lit("webrequests") as "filetype", lit(Pipeline2To3.STAGE_NAME) as "stage", $"collecttype",$"engine",$"dt",$"filedt").distinct())
      .repartition(1)
      .write.mode(SaveMode.Overwrite)
      .parquet(context.dirin())
  }
}
