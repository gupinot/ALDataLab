/**
  * Created by guillaumepinot on 28/11/2015.
  */
package com.alstom.datalab

import org.apache.spark.sql.SQLContext

class WebApp(DIN:String) {

  def genAgg(sqlContext: SQLContext, DirIn:String, DirOut: String): Unit = {
    //Construct aggregated data for dataLabWebApp

    val df5 = sqlContext.read.parquet(DirIn)

    /*val dfAggFull = df5
      .groupBy("source_site", "dest_site", "source_sector", "source_I_ID_site",
      "enddate", "source_app_name", "source_app_category", "source_app_company", "source_app_exec", "source_app_paths",
      "engine", "fileenginedate", "dest_ip", "dest_port", "con_protocol", "con_status", "url")
      .agg(sum("con_number"))*/


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
