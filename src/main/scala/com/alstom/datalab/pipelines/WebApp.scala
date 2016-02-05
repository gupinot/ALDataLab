/**
  * Created by guillaumepinot on 28/11/2015.
  */
package com.alstom.datalab.pipelines

import com.alstom.datalab.{Meta, ControlFile, Pipeline}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

class WebApp(implicit sqlContext: SQLContext) extends Pipeline with Meta {

  import sqlContext.implicits._

  def aggregateDevice(df: DataFrame) = {

  }

  def generate(df: DataFrame, collecttype: String = "device") = {
    val dayone = "2015-10-01"
    val date_last =

  }

  def execute(): Unit = {


    val jobidcur:Long = System.currentTimeMillis/1000

    //read meta to compute
    val meta45 = aggregateMeta(loadMeta(context.meta()), Pipeline4To5.STAGE_NAME).as("meta45")

    //keep only days with sufficient engines collected : 20 for device, 3 for server
    val dt_device = meta45
      .filter("collecttype = device")
      .groupBy("dt")
      .agg(countDistinct($"engine").as("engine_count"))
      .filter("engine_count >= 20")
      .select($"dt")
      .distinct()
      .collect()
      .map(_.getDate(0))
    val dt_server = meta45
      .filter("collecttype = server")
      .groupBy("dt")
      .agg(countDistinct($"engine").as("engine_count"))
      .filter("engine_count >= 3")
      .select($"dt")
      .distinct()
      .collect()
      .map(_.getDate(0))

    val aggregated = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}")
    aggregated.cache()

    val aggregated_device = aggregated.filter($"dt".isin(dt_device:_*))
    val aggregated_server = aggregated.filter($"dt".isin(dt_server:_*))

    generate(aggregated_device)
    generate(aggregated_server)


    /*stat <- NXFile[, list(sum_con_cardinality=sum(NX_con_cardinality),
      sum_con_out_traffic=sum(as.numeric(NX_con_out_traffic)),
      sum_con_in_traffic=sum(as.numeric(NX_con_in_traffic)),
      mean_con_duration=round(mean(NX_con_duration), digits=0),
      count_unique_Login=length(unique(I_ID)),
      count_unique_device_name=length(unique(NX_device_name)),
      I_ID=paste(unique(I_ID), collapse=",")),
    by=list(REFIPRANGE_device_last_Site,
      REFIPRANGE_con_destination_Site,
      IDM_Sector,
      IDM_SiteCode,
      NX_con_end_date,
      NX_bin_app_name,
      NX_bin_app_category,
      NX_bin_app_company,
      NX_bin_exec_name,
      NX_source_file_name,
      NX_con_destination_ip,
      NX_con_port,
      NX_con_type,
      NX_con_status,
      wr.url)]*/
  }
}

