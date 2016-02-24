package com.alstom.datalab.pipelines

import com.alstom.datalab.Pipeline
import com.alstom.datalab.Util._
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.functions._


class Flow(implicit sqlContext: SQLContext) extends Pipeline {
  import sqlContext.implicits._

  override def execute(): Unit = {

    val nxdeviceflow = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.diragg()}/device")
    val nxserverflow = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.diragg()}/server")
    val linuxserverflow = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.diragg()}/serversockets")

    val myConcat = new ConcatUniqueString(",")

    val serverflow_detail =nxdeviceflow
      .filter($"con_status" === "closed")
      .groupBy(
        lit("nxdevice") as "collecttype",
        lit("client").as("remote_type"),
        $"dt", $"month",
        $"dest_ip" as "server_ip", $"dest_site" as "server_site", $"dest_aip_app_name" as "server_aip_app_name", $"dest_aip_app_sector" as "server_sector",
        $"source_sector".as("remote_sector"),
        $"source_I_ID_site".as("remote_site"),
        lit(null).as("remote_ip"),
        $"source_app_exec".as("remote_app_exec"),
        lit(null).as("remote_aip_app_name"),
        $"dest_port".as("server_port"),
        $"con_protocol".as("con_protocol"))
      .agg(sum($"con_number").as("con_number"),
        sum($"con_traffic_in" + $"con_traffic_out").as("con_traffic"),
        myConcat($"I_ID_U").as("client_user"))
      .withColumn("distinct_client_user", countSeparator(",")($"client_user"))
      .unionAll(nxserverflow
        .filter($"con_status" === "closed")
        .groupBy(
          lit("nxserver") as "collecttype",
          lit("client").as("remote_type"),
          $"dt", $"month",
          $"dest_ip" as "server_ip",$"dest_site" as "server_site", $"dest_aip_app_name" as "server_aip_app_name", $"dest_aip_app_sector" as "server_sector",
          $"source_aip_app_sector".as("remote_sector"),
          $"source_site".as("remote_site"),
          $"source_ip".as("remote_ip"),
          $"source_app_exec".as("remote_app_exec"),
          $"source_aip_app_name".as("remote_aip_app_name"),
          $"dest_port".as("server_port"),
          $"con_protocol".as("con_protocol"))
        .agg(sum($"con_number").as("con_number"),
          sum($"con_traffic_in" + $"con_traffic_out").as("con_traffic"))
        .withColumn("client_user", lit(null))
        .withColumn("distinct_client_user", lit(null))
      )
      .unionAll(nxserverflow
        .filter($"con_status" === "closed")
        .groupBy(
          lit("nxserver") as "collecttype",
          lit("server").as("remote_type"),
          $"dt", $"month",
          $"source_ip" as "server_ip",$"source_site" as "server_site", $"source_aip_app_name" as "server_aip_app_name", $"source_aip_app_sector" as "server_sector",
          $"dest_aip_app_sector".as("remote_sector"),
          $"dest_site".as("remote_site"),
          $"dest_ip".as("remote_ip"),
          $"source_app_exec".as("remote_app_exec"),
          $"dest_aip_app_name".as("remote_aip_app_name"),
          $"dest_port".as("server_port"),
          $"con_protocol".as("conf_protocol"))
        .agg(sum($"con_number").as("con_number"),
          sum($"con_traffic_in" + $"con_traffic_out").as("con_traffic"))
        .withColumn("client_user", lit(null))
        .withColumn("distinct_client_user", lit(null))
      )
      .unionAll(linuxserverflow
        .filter($"con_status" === "closed")
        .groupBy(
          lit("linuxserver") as "collecttype",
          lit("client").as("remote_type"),
          $"dt", $"month",
          $"dest_ip" as "server_ip",$"dest_site" as "server_site", $"dest_aip_app_name" as "server_aip_app_name", $"dest_aip_app_sector" as "server_sector",
          $"source_aip_app_sector".as("remote_sector"),
          $"source_site".as("remote_site"),
          $"source_ip".as("remote_ip"),
          $"source_app_exec".as("remote_app_exec"),
          $"source_aip_app_name".as("remote_aip_app_name"),
          $"dest_port".as("server_port"),
          $"con_protocol".as("con_protocol"))
        .agg(sum($"con_number").as("con_number"),
          sum($"con_traffic_in" + $"con_traffic_out").as("con_traffic"))
        .withColumn("client_user", lit(null))
        .withColumn("distinct_client_user", lit(null))
      )
      .unionAll(linuxserverflow
        .filter($"con_status" === "closed")
        .groupBy(
          lit("linuxserver") as "collecttype",
          lit("server").as("remote_type"),
          $"dt", $"month",
          $"source_ip" as "server_ip",$"source_site" as "server_site", $"source_aip_app_name" as "server_aip_app_name", $"source_aip_app_sector" as "server_sector",
          $"dest_aip_app_sector".as("remote_sector"),
          $"dest_site".as("remote_site"),
          $"dest_ip".as("remote_ip"),
          $"source_app_exec".as("remote_app_exec"),
          $"dest_aip_app_name".as("remote_aip_app_name"),
          $"dest_port".as("server_port"),
          $"con_protocol".as("con_protocol"))
        .agg(sum($"con_number").as("con_number"),
          sum($"con_traffic_in" + $"con_traffic_out").as("con_traffic"))
        .withColumn("client_user", lit(null))
        .withColumn("distinct_client_user", lit(null))
      )

    serverflow_detail.write.mode(SaveMode.Overwrite)
      .partitionBy("month").parquet(s"${context.diragg()}/flow_detail_day")


    serverflow_detail
      .groupBy("month",
        "server_ip", "server_site", "server_aip_app_name", "server_aip_app_sector",
        "remote_type", "collecttype",
        "remote_site", "remote_sector")
      .agg(countDistinct($"remote_ip").as("count_distinct_remote_ip"),
        countDistinct($"remote_app_exec").as("count_distinct_remote_app_exec"),
        sum($"con_traffic").as("con_traffic"),
        sum($"con_number").as("con_number"),
        myConcat($"client_user").as("client_user")
      )
      .withColumn("distinct_client_user", countSeparator(",")($"client_user"))
      .drop($"client_user")
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .partitionBy("month").parquet(s"${context.diragg()}/flow_month")

    serverflow_detail
      .groupBy("server_ip", "server_site", "server_aip_app_name", "server_aip_app_sector",
        "remote_type", "collecttype",
        "remote_site", "remote_sector")
      .agg(countDistinct($"remote_ip").as("count_distinct_remote_ip"),
        countDistinct($"remote_app_exec").as("count_distinct_remote_app_exec"),
        sum($"con_traffic").as("con_traffic"),
        sum($"con_number").as("con_number"),
        myConcat($"client_user").as("client_user")
      )
      .withColumn("distinct_client_user", countSeparator(",")($"client_user"))
      .drop($"client_user")
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .partitionBy("month").parquet(s"${context.diragg()}/flow")
  }
}
