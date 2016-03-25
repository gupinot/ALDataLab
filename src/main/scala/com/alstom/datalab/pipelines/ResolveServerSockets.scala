package com.alstom.datalab.pipelines

import com.alstom.datalab.Util._
import com.alstom.datalab.{MetaSockets, Pipeline}
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Created by guillaumepinot on 16/02/2016.
  */

object ResolveServerSockets {
  val STAGE_NAME = "resolvesockets"
}



class ResolveServerSockets (implicit sqlContext: SQLContext) extends Pipeline with MetaSockets {
  import sqlContext.implicits._


  override def execute(): Unit = {

    //read meta to compute
    val meta_delta_ok = deltaMetaEncodedToResolved(context.meta())
    val meta = broadcast(meta_delta_ok)

    val all_dt = {
      if (this.inputFiles.length == 2) {
        val begindate = this.inputFiles(0)
        val enddate = this.inputFiles(1)
        meta.select(to_date($"dt").as("dt")).filter($"dt" >= begindate).filter($"dt" <= enddate).distinct().collect().map(_.getDate(0))
      } else {
        meta.select(to_date($"dt").as("dt")).distinct().collect().map(_.getDate(0))
      }
    }

    if (all_dt.length > 0) {
      //read lsof and ps parquet
      val lsof_parquet = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/lsof")
        .filter($"status" !== "")
        .filter(to_date($"dt").isin(all_dt: _*))
      val ps_parquet = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/ps")
        .filter(to_date($"dt").isin(all_dt: _*))

      //filter lsof and ps on meta to compute
      val lsof_filtered = lsof_parquet
        .join(meta, lsof_parquet("server_ip") === meta("server_ip")
          && lsof_parquet("dt") === meta("dt"), "inner")
        .select(lsof_parquet.columns.map(lsof_parquet.col): _*).as("lsof")

      val ps_filtered = ps_parquet
        .join(meta, ps_parquet("server_ip") === meta("server_ip")
          && ps_parquet("dt") === meta("dt"), "inner")
        .select(ps_parquet.columns.map(ps_parquet.col): _*).as("ps")

      //resolve ps for lsof
      val lsof_resolved1 = lsof_filtered
        .join(ps_filtered, $"lsof.dt" === $"ps.dt"
          && $"lsof.server_ip" === $"ps.server_ip"
          && $"lsof.pid" === $"ps.pid", "left_outer")
        .select((lsof_filtered.columns.map(lsof_filtered.col)
          ++ List($"ps.command" as "command_detail")): _*)

      //list listen ports for each server_ip and dt
      val lsof_listen = lsof_resolved1
        .filter($"status" === "(LISTEN)")
        .withColumn("port_listen", regexp_replace($"name", "(.*):(.*)", "$2"))
        .select("dt", "server_ip", "port_listen")
        .withColumn("dest", lit(true))
        .distinct()

      //extract name from lsof and join with listen
      val lsof_resolved2 = lsof_resolved1
        .filter($"status" !== "(LISTEN)")
        .withColumn("con_local_ip", regexp_replace($"name", "(.*):(.*)->(.*):(.*)", "$1"))
        .withColumn("con_local_port", regexp_replace($"name", "(.*):(.*)->(.*):(.*)", "$2"))
        .withColumn("con_foreign_ip", regexp_replace($"name", "(.*):(.*)->(.*):(.*)", "$3"))
        .withColumn("con_foreign_port", regexp_replace($"name", "(.*):(.*)->(.*):(.*)", "$4"))
        .join(lsof_listen, lsof_resolved1("dt") === lsof_listen("dt")
          && lsof_resolved1("server_ip") === lsof_listen("server_ip")
          && $"con_local_port" === lsof_listen("port_listen"), "left_outer")
        .select((lsof_resolved1.columns.map(lsof_resolved1.col) ++ List($"dest", $"con_local_ip", $"con_local_port", $"con_foreign_ip", $"con_foreign_port")): _*)

      //resolve source and dest according to listen
      val lsof_resolved3 = lsof_resolved2
        .filter($"dest" === true)
        .withColumn("source_ip", $"con_foreign_ip")
        .withColumn("dest_ip", $"con_local_ip")
        .withColumn("dest_port", $"con_local_port")
        .withColumn("source_app_exec", lit(null))
        .withColumn("source_app_paths", lit(null))
        .drop($"dest")
        .unionAll(lsof_resolved2
          .filter("dest is null")
          .withColumn("source_ip", $"con_local_ip")
          .withColumn("dest_ip", $"con_foreign_ip")
          .withColumn("dest_port", $"con_foreign_port")
          .withColumn("source_app_exec", $"command")
          .withColumn("source_app_paths", $"command_detail")
          .drop($"dest")
        )
        .filter($"source_ip" !== $"dest_ip")

      //encode status udf
      def encodeStatus = udf(
        (status: String) => status match {
          case "(ESTABLISHED)" | "(CLOSE-WAIT)" | "(FIN-WAIT-1)" | "(FIN-WAIT-2)" | "(CLOSING)" | "(TIME-WAIT)" => "closed"
          case "(SYN-SENT)" => "SYN-SENT"
          case "(SYN-RECEIVED)" => "RECEIVED"
          case _ => null
        }
      )

      //format output like nexthink's server resolved output
      val lsof_resolved4 = lsof_resolved3.select(
        lit("NA") as "I_ID_U",
        lit("NA") as "I_ID_D", //TODO : resolve with hostname
        lit("NA") as "con_start",
        $"dt" as "con_end",
        lit("NA") as "con_duration",
        lit(1) as "con_number",
        $"dest_ip" as "dest_ip",
        lit("NA") as "con_traffic_out",
        lit("NA") as "con_traffic_in",
        $"node" as "con_protocol",
        encodeStatus($"status") as "con_status",
        $"dest_port" as "dest_port",
        lit("NA") as "source_app_category",
        lit("NA") as "source_app_company",
        lit("NA") as "source_app_name",
        $"source_app_exec" as "source_app_exec",
        $"source_app_paths" as "source_app_paths",
        lit("NA") as "source_app_version",
        $"source_ip" as "source_ip",
        $"server_ip" as "engine",
        lit("NA") as "filedt",
        lit("linux") as "collecttype",
        to_date($"dt") as "dt",
        lit("NA") as "source_sector",
        lit("NA") as "source_I_ID_site",
        lit("NA") as "url",
        $"month" as "month")
        .filter("con_status is not null")
        .filter(regexudf(ipinternalpattern)($"source_ip"))
        .filter(regexudf(ipinternalpattern)($"dest_ip"))

      val lsof_final = resolveSite(resolveAIP(lsof_resolved4))

      //save resolved results
      lsof_final.write.mode(SaveMode.Append)
        .partitionBy("month").parquet(s"${context.dirout()}")

      //write meta
      val meta_result = meta.withColumn("stage", lit(ResolveServerSockets.STAGE_NAME))
        .select("filetype", "stage", "server_ip", "dt")
        .filter(to_date($"dt").isin(all_dt: _*))
        .repartition(1)
      meta_result.write.mode(SaveMode.Append).parquet(context.meta())
    }
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
    df.withColumn("source_ip_int",aton($"source_ip"))
      .withColumn("dest_ip_int",aton($"dest_ip"))
      .join(dfIP.as("sourceIP"), $"source_ip_int" === $"sourceIP.mdm_ip","left_outer")
      .join(dfIP.as("destIP"), $"dest_ip_int" === $"destIP.mdm_ip","left_outer")
      .select((df.columns.toList.map(df.col)
        ++ List(formatSite($"sourceIP.site").as("source_site"),formatSite($"destIP.site").as("dest_site"))):_*)
  }

  def resolveAIP(df: DataFrame): DataFrame = {

    val dfAIP = broadcast(context.repo().readAIP().cache())

    df.join(dfAIP.as("destAip"), $"dest_ip" === $"destAip.aip_server_ip", "left_outer")
      .join(dfAIP.as("sourceAip"), $"source_ip" === $"sourceAip.aip_server_ip", "left_outer")
      .select((df.columns.toList.map(df.col) ++
        List($"destAip.aip_server_adminby" as "dest_aip_server_adminby",
          $"destAip.aip_server_hostname" as "dest_aip_server_hostname",
          $"destAip.aip_server_function" as "dest_aip_server_function",
          $"destAip.aip_server_subfunction" as "dest_aip_server_subfunction",
          $"destAip.aip_app_name" as "dest_aip_app_name",
          $"destAip.aip_app_type" as "dest_aip_app_type",
          $"destAip.aip_appinstance_type" as "dest_aip_appinstance_type",
          $"destAip.aip_app_state" as "dest_aip_app_state",
          $"destAip.aip_app_sensitive" as "dest_aip_app_sensitive",
          $"destAip.aip_app_criticality" as "dest_aip_app_criticality",
          $"destAip.aip_app_sector" as "dest_aip_app_sector",
          $"destAip.aip_appinstance_sector" as "dest_aip_appinstance_sector",
          $"destAip.aip_appinstance_state" as "dest_aip_appinstance_state",
          $"destAip.aip_app_shared_unique_id" as "dest_aip_app_shared_unique_id",
          $"sourceAip.aip_server_adminby" as "source_aip_server_adminby",
          $"sourceAip.aip_server_hostname" as "source_aip_server_hostname",
          $"sourceAip.aip_server_function" as "source_aip_server_function",
          $"sourceAip.aip_server_subfunction" as "source_aip_server_subfunction",
          $"sourceAip.aip_app_name" as "source_aip_app_name",
          $"sourceAip.aip_app_type" as "source_aip_app_type",
          $"sourceAip.aip_appinstance_type" as "source_aip_appinstance_type",
          $"sourceAip.aip_app_state" as "source_aip_app_state",
          $"sourceAip.aip_app_sensitive" as "source_aip_app_sensitive",
          $"sourceAip.aip_app_criticality" as "source_aip_app_criticality",
          $"sourceAip.aip_app_sector" as "source_aip_app_sector",
          $"sourceAip.aip_appinstance_sector" as "source_aip_appinstance_sector",
          $"sourceAip.aip_appinstance_state" as "source_aip_appinstance_state",
          $"sourceAip.aip_app_shared_unique_id" as "source_aip_app_shared_unique_id")):_*
      )
  }
}
