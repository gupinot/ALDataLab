package com.alstom.datalab.pipelines

import java.text.SimpleDateFormat
import java.util.Date

import com.alstom.datalab.Util._
import com.alstom.datalab.{MetaSockets, Meta, Pipeline}
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Created by guillaumepinot on 22/01/2016.
  */

object AggregateServerSockets {
    val STAGE_NAME = "aggregatesockets"
}


class AggregateServerSockets(implicit sqlContext: SQLContext) extends Pipeline with MetaSockets {

    import sqlContext.implicits._

    def execute(): Unit = {


        val jobidcur:Long = System.currentTimeMillis/1000
      //read meta to compute
        val meta_delta_ok = deltaMetaResolvedToAggregated(context.meta())

        val meta = meta_delta_ok.cache()

        val resolved = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}")

        //Read resolved to compute
        val resolved_filtered = resolved
          .join(meta, resolved("engine") === meta("server_ip")
            && resolved("dt") === meta("dt"), "inner")
          .select(resolved.columns.map(resolved.col): _*).as("resolved")

        val resAgregatedServer = resolved_filtered
          .filter($"collecttype" === "linux")
          .groupBy("source_sector", "source_ip", "source_aip_server_hostname", "source_site", "source_app_name", "source_app_category", "source_app_exec", "url",
              "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type", "source_aip_app_sector",
              "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
              "dest_ip", "dest_site", "dest_port", "con_protocol", "con_status", "dest_aip_server_hostname",
              "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type", "dest_aip_app_sector",
              "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type", "dt", "month", "engine", "filedt")
          .agg(sum($"con_number").cast("bigint").as("con_number"),
              sum($"con_traffic_in").cast("bigint").as("con_traffic_in"),
              sum($"con_traffic_out").cast("bigint").as("con_traffic_out"),
              mean($"con_duration").as("con_duration"))
          .write.mode(SaveMode.Append)
          .partitionBy("month").parquet(s"${context.dirout()}")

        val meta_result = meta.withColumn("stage", lit(AggregateServerSockets.STAGE_NAME))
          .select("filetype", "stage", "server_ip", "dt")
          .repartition(1)
        meta_result.write.mode(SaveMode.Append).parquet(context.meta())

    }
}