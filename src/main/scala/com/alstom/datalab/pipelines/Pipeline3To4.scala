package com.alstom.datalab.pipelines

import com.alstom.datalab.{Meta, Pipeline, ControlFile}
import com.alstom.datalab.Util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.functions._


/**
  * Created by guillaumepinot on 05/11/2015.
  */
object Pipeline3To4 {
  val STAGE_NAME = "pipe3to4"
}

class Pipeline3To4(implicit sqlContext: SQLContext) extends Pipeline with Meta {
  import sqlContext.implicits._

  def execute(): Unit = {

    val jobidcur:Long = System.currentTimeMillis/1000

    //read meta 23 and 34
    val cnx_meta_delta_ok = deltaMeta(context.meta(), Pipeline2To3.STAGE_NAME, Pipeline3To4.STAGE_NAME)

    val cnx = broadcast(cnx_meta_delta_ok)

    val all_dt = {
      if (this.inputFiles.length == 2) {
        val begindate = this.inputFiles(0)
        val enddate = this.inputFiles(1)
        cnx.select($"dt").filter($"dt" >= begindate).filter($"dt" <= enddate).distinct().collect().map(_.getDate(0))
      }else {
        cnx.select($"dt").distinct().collect().map(_.getDate(0))
      }
    }

    // main dataframes
    val cnx_parquet = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/connection/")
    val wr_parquet = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/webrequest/")


    //Read and resolve
    val cnx_filtered = cnx_parquet.filter($"dt".isin(all_dt:_*))
      .join(cnx, cnx_parquet("collecttype") === cnx("collecttype") && cnx_parquet("engine") === cnx("engine")
        && cnx_parquet("filedt") === cnx("min_filedt") && cnx_parquet("dt") === cnx("dt"), "inner")
      .select(cnx_parquet.columns.map(cnx_parquet.col):_*)

    val wr_filtered = wr_parquet.filter($"dt".isin(all_dt:_*))
      .join(cnx, wr_parquet("collecttype") === cnx("collecttype") && wr_parquet("engine") === cnx("engine")
        && wr_parquet("filedt") === cnx("min_filedt") && wr_parquet("dt") === cnx("dt"), "inner")
      .select(wr_parquet.columns.map(wr_parquet.col):_*)

    println("pipeline3to4() : data resolutions")
    val cnx_resolved = resolveSite(resolveAIP(resolveSector(cnx_filtered)))

    wr_filtered.registerTempTable("webrequests")
    cnx_resolved.registerTempTable("connections")

    //join with df
    sqlContext.sql(
      s"""
         |with weburl as (
         |   select collecttype, dt, engine, filedt, I_ID_D,source_app_name,dest_ip,dest_port,concat_ws('|',collect_set(url)) as url
         |   from webrequests
         |   group by collecttype, dt, engine, filedt, I_ID_D,source_app_name,dest_ip,dest_port
         |)
         |select c.*, w.url
         |from connections c
         |left outer join weburl w on (
         |  c.collecttype=w.collecttype
         |  and c.dt=w.dt
         |  and c.engine=w.engine
         |  and c.filedt=w.filedt
         |  and c.I_ID_D=w.I_ID_D
         |  and c.source_app_name=w.source_app_name
         |  and c.dest_port=w.dest_port
         |  and c.dest_ip=w.dest_ip
         |)
      """.stripMargin)
      .write.mode(SaveMode.Append)
      .partitionBy("month").parquet(context.dirout())

    val cnx_meta_result = cnx_meta_delta_ok.withColumn("stage", lit(Pipeline3To4.STAGE_NAME))
      .withColumnRenamed("min_filedt", "filedt")
      .select("filetype", "stage", "collecttype", "engine", "dt", "filedt")
      .repartition(1)
    cnx_meta_result.write.mode(SaveMode.Append).parquet(context.meta())

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

  def resolveSector(df: DataFrame): DataFrame = {
    val dfI_ID = broadcast(context.repo().readI_ID().select("I_ID","Sector","SiteCode").cache())

    //join for resolution
    df.join(dfI_ID.as("dfID"), df("I_ID_U") === dfI_ID("I_ID"), "left_outer")
      .select((df.columns.toList.map(df.col)
        ++ List($"dfID.Sector".as("source_sector"),formatSite($"dfID.SiteCode").as("source_I_ID_site"))):_*)
  }

  def resolveAIP(df: DataFrame): DataFrame = {

    val dfAIP = broadcast(context.repo().readAIP().cache())

    df.join(dfAIP.as("destAip"), $"dest_ip" === $"destAip.aip_server_ip", "left_outer")
      .join(dfAIP.as("sourceAip"), $"source_ip" === $"sourceAip.aip_server_ip", "left_outer")
      .select((df.columns.toList.map(df.col) ++
        List($"destAip.aip_server_adminby" as "dest_aip_server_adminby",
        $"destAip.aip_server_function" as "dest_aip_server_function",
        $"destAip.aip_server_subfunction" as "dest_aip_server_subfunction",
        $"destAip.aip_app_name" as "dest_aip_app_name",
        $"destAip.aip_app_type" as "dest_aip_app_type",
        $"destAip.aip_appinstance_type" as "dest_aip_appinstance_type",
        $"destAip.aip_app_state" as "dest_aip_app_state",
        $"destAip.aip_app_sensitive" as "dest_aip_app_sensitive",
        $"destAip.aip_app_criticality" as "dest_aip_app_criticality",
        $"destAip.aip_app_sector" as "dest_aip_app_sector",
        $"destAip.aip_app_shared_unique_id" as "dest_aip_app_shared_unique_id",
        $"sourceAip.aip_server_adminby" as "source_aip_server_adminby",
        $"sourceAip.aip_server_function" as "source_aip_server_function",
        $"sourceAip.aip_server_subfunction" as "source_aip_server_subfunction",
        $"sourceAip.aip_app_name" as "source_aip_app_name",
        $"sourceAip.aip_app_type" as "source_aip_app_type",
        $"sourceAip.aip_appinstance_type" as "source_aip_appinstance_type",
        $"sourceAip.aip_app_state" as "source_aip_app_state",
        $"sourceAip.aip_app_sensitive" as "source_aip_app_sensitive",
        $"sourceAip.aip_app_criticality" as "source_aip_app_criticality",
        $"sourceAip.aip_app_sector" as "source_aip_app_sector",
        $"sourceAip.aip_app_shared_unique_id" as "source_aip_app_shared_unique_id")):_*
      )
  }
}
