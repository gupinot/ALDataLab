package com.alstom.datalab.pipelines

import com.alstom.datalab.Util._
import com.alstom.datalab.{MetaSockets, Pipeline}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

/**
  * Created by guillaumepinot on 16/02/2016.
  */

object ResolveServerSockets {
  val STAGE_NAME = "resolvesockets"
}



class ResolveServerSockets (implicit sqlContext: SQLContext) extends Pipeline with MetaSockets {
  import sqlContext.implicits._


  override def execute(): Unit = {
    val meta_delta_ok = deltaMetaEncodedToResolved(context.meta())

    val meta = broadcast(meta_delta_ok)


    val lsof_parquet = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/lsof/")
    val ps_parquet = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/ps/")

    val lsof_filtered = lsof_parquet
      .join(meta, lsof_parquet("server_ip") === meta("server_ip")
        && lsof_parquet("dt") === meta("dt"), "inner")
      .select(lsof_parquet.columns.map(lsof_parquet.col): _*).as("lsof")

    val ps_filtered = ps_parquet
      .join(meta, ps_parquet("server_ip") === meta("server_ip")
        && ps_parquet("dt") === meta("dt"), "inner")
      .select(ps_parquet.columns.map(ps_parquet.col): _*)


    //resolve ps
    val lsof_resolved1 = lsof_filtered
      .join(ps_filtered.as("ps"), $"lsof.dt" === $"ps.dt"
        && $"lsof.server_ip" === $"ps.server_ip"
        && $"lsof.pid" === $"ps.pid", "left_outer")
      .select((lsof_filtered.columns.map(lsof_filtered.col)
        ++ List($"ps.command" as "command_detail")): _*)
      .filter($"status" !== "")

    //identify LISTEN port list by server
    val lsof_listen = lsof_resolved1
      .filter($"status" === "(LISTEN)")
      .withColumn("port_listen", regexp_replace($"name", "(.*):(.*)", "$2"))
      .select("dt", "server_ip", "port_listen")
      .distinct()

    val lsof_resolved2 = lsof_resolved1
      .filter($"status" !== "(LISTEN)")
      .withColumn("con_local_ip", regexp_replace($"name", "(.*):(.*)->(.*):(.*)", "$1"))
      .withColumn("con_local_port", regexp_replace($"name", "(.*):(.*)->(.*):(.*)", "$2"))
      .withColumn("con_foreign_ip", regexp_replace($"name", "(.*):(.*)->(.*):(.*)", "$3"))
      .withColumn("con_foreign_port", regexp_replace($"name", "(.*):(.*)->(.*):(.*)", "$4"))



    //val lsof_resolved2 = resolveSite(resolveAIP(lsof_resolved1))



  }

    def buildIpLookupTable(): DataFrame = {
      val ip = context.repo().readMDM().select($"mdm_loc_code", explode(range($"mdm_ip_start_int", $"mdm_ip_end_int")).as("mdm_ip"))
      ip.registerTempTable("ip")
      broadcast(sqlContext.sql("""select mdm_ip, concat_ws('_',collect_set(mdm_loc_code)) as site from ip group by mdm_ip""").cache())
    }

    def resolveSite(df: DataFrame): DataFrame = {

      //Read MDM repository file
      val dfIP = buildIpLookupTable()

      //Resolve Site
      df.withColumn("server_ip_int",aton($"server_ip"))
        .join(dfIP.as("serverIP"), $"server_ip_int" === $"serverIP.mdm_ip","left_outer")
        .select((df.columns.toList.map(df.col)
          ++ List(formatSite($"serverIP.site").as("server_site"))):_*)
    }

    def resolveAIP(df: DataFrame): DataFrame = {

      val dfAIP = broadcast(context.repo().readAIP().cache())

      df.join(dfAIP.as("serverAip"), $"server_ip" === $"serverAip.aip_server_ip", "left_outer")
        .select((df.columns.toList.map(df.col) ++
          List($"serverAip.aip_server_adminby" as "server_aip_server_adminby",
            $"serverAip.aip_server_function" as "server_aip_server_function",
            $"serverAip.aip_server_subfunction" as "server_aip_server_subfunction",
            $"serverAip.aip_app_name" as "server_aip_app_name",
            $"serverAip.aip_app_type" as "server_aip_app_type",
            $"serverAip.aip_appinstance_type" as "server_aip_appinstance_type",
            $"serverAip.aip_app_state" as "server_aip_app_state",
            $"serverAip.aip_app_sensitive" as "server_aip_app_sensitive",
            $"serverAip.aip_app_criticality" as "server_aip_app_criticality",
            $"serverAip.aip_app_sector" as "server_aip_app_sector",
            $"serverAip.aip_app_shared_unique_id" as "server_aip_app_shared_unique_id")):_*
        )
    }
}
