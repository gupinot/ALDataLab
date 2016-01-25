package com.alstom.datalab.pipelines


import com.alstom.datalab.{Meta, Pipeline, ControlFile}
import com.alstom.datalab.Util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Created by guillaumepinot on 22/01/2016.
  */

object Pipeline4To5 {
    val STAGE_NAME = "pipe4to5"
}

class Pipeline4To5(implicit sqlContext: SQLContext) extends Pipeline with Meta {

  import sqlContext.implicits._

  def execute(): Unit = {


    val jobidcur:Long = System.currentTimeMillis/1000

    //read meta to compute
    val meta_delta_ok = deltaMeta4to5(context.meta())

    val meta = broadcast(meta_delta_ok)

    val all_dt = {
      if (this.inputFiles.length == 2) {
        val begindate = this.inputFiles(0)
        val enddate = this.inputFiles(1)
        meta.select($"dt").filter($"dt" >= begindate).filter($"dt" <= enddate).distinct().collect().map(_.getDate(0))
      }else {
        meta.select($"dt").distinct().collect().map(_.getDate(0))
      }
    }

    val resolved = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}")

    //Read resolved to compute
    val resolved_filtered = resolved.filter($"dt".isin(all_dt:_*))
      .join(meta, resolved("collecttype") === meta("collecttype") && resolved("engine") === meta("engine")
        && resolved("filedt") === meta("min_filedt") && resolved("dt") === meta("dt"), "inner")
      .select(resolved.columns.map(resolved.col):_*)

    val resAgregatedServer = resolved_filtered
      .filter($"collecttype" === "server")
      .groupBy("source_ip", "source_site", "source_app_name", "source_app_category", "source_app_exec", "url",
      "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type", "source_aip_app_sector",
      "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
      "dest_ip", "dest_site", "dest_port", "con_protocol",
      "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type", "dest_aip_app_sector",
      "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type", "dt", "month")
      .agg(sum($"con_number").as("con_number"),
        sum($"con_traffic_in").as("con_traffic_in"),
        sum($"con_traffic_out").as("con_traffic_out"),
        mean($"con_duration").as("con_duration"))
      .write.mode(SaveMode.Append)
      .partitionBy("month").parquet(s"${context.dirout()}/server")

    val myConcat = new ConcatString(",")
    val resAgregatedDevice = resolved_filtered
      .filter($"collecttype" === "device")
      .groupBy("source_I_ID_site", "source_site", "source_app_name", "source_app_category", "source_app_exec", "url",
        "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type", "source_aip_app_sector",
        "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
        "dest_ip", "dest_site", "dest_port", "con_protocol",
        "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type", "dest_aip_app_sector",
        "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type", "dt", "month")
      .agg(sum($"con_number").as("con_number"),
        sum($"con_traffic_in").as("con_traffic_in"),
        sum($"con_traffic_out").as("con_traffic_out"),
        mean($"con_duration").as("con_duration"),
        myConcat($"I_ID_U").as("I_ID_U"),
        approxCountDistinct($"I_ID_U").as("distinct_I_ID_U"))
      .write.mode(SaveMode.Append)
      .partitionBy("month").parquet(s"${context.dirout()}/device")

    val meta_result = meta_delta_ok.withColumn("stage", lit(Pipeline4To5.STAGE_NAME))
      .withColumnRenamed("min_filedt", "filedt")
      .select("filetype", "stage", "collecttype", "engine", "dt", "filedt")
      .filter($"dt".isin(all_dt:_*))
      .repartition(1)
    meta_result.write.mode(SaveMode.Append).parquet(context.meta())
  }
}
