package com.alstom.datalab.pipelines

import com.alstom.datalab.exception.MissingWebRequestException
import com.alstom.datalab.{Pipeline, Repo}
import com.alstom.datalab.Util._
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._


/**
  * Created by guillaumepinot on 05/11/2015.
  */
object Pipeline3To4 {
  val STAGE_NAME = "pipe3to4"
}

class Pipeline3To4(sqlContext: SQLContext) extends Pipeline {
  import sqlContext.implicits._

  var AIPToResolve = true

  def execute(): Unit = {

    val jobidcur:Long = System.currentTimeMillis/1000
    val sc = sqlContext.sparkContext

    // main dataframes
    val cnx_parquet = sqlContext.read.option("mergeSchema", "false").parquet(s"$dirin/connection/")
    val wr = sqlContext.read.option("mergeSchema", "false").parquet(s"$dirin/webrequest/")

    // metadata view on main dataframes
    val cnx_meta = cnx_parquet.select($"collecttype" as "type",$"engine",$"dt",$"filedt")
    val wr_meta = wr.select($"collecttype" as "type",$"engine",$"dt",$"filedt").distinct()

    //read control files from inputFiles and filter on connection filetype)
    val control: DataFrame = this.inputFiles.map(filein => {sc.textFile(filein)})
      .reduce(_.union(_))
      .map(_.split(";"))
      .map(s => ControlFile(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7)))
      .toDF()

    val jobidorig=control.select("jobid").distinct().head.getString(0)

    val ctl_cnx_only = control
      .filter($"filetype" === "connection")
      .select($"collecttype" as "type", $"engine" as "engine", to_date($"day") as "dt", to_date($"filedt") as "filedt")

    //select correct filedt from cn-onnections
    val cnx_best = cnx_meta.groupBy("type", "dt", "engine").agg(min(to_date($"filedt")) as "filedt")

    //select correct filedt from dfControlConnection
    val cnx_all = cnx_best.as("cnx_best")
      .join(ctl_cnx_only.as("ctl"),
      ($"cnx_best.type" === $"ctl.type") && ($"cnx_best.engine" === $"ctl.engine")
        && ($"cnx_best.dt" === $"ctl.dt") && ($"cnx_best.filedt" === $"ctl.filedt"), "inner")
      .join(wr_meta.as("wr"),
      ($"cnx_best.type" === $"wr.type") && ($"cnx_best.engine" === $"wr.engine")
        && ($"cnx_best.filedt" === $"wr.filedt") && ($"cnx_best.dt" === $"wr.dt"), "left_outer")
    .select($"cnx_best.dt", $"ctl.filedt", $"ctl.type", $"ctl.engine", $"wr.filedt" as "wr_filedt")

    // save incomplete connections data to reject file
    val cnx_no_wr = cnx_all.filter($"wr_filedt" === null)
    if (cnx_no_wr .count() != 0) {
        println("following webrequest files not found :")
        cnx_no_wr.select($"type", $"engine", $"filedt", $"dt",lit("webrequest not found") as "Status")
          .write.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", ";").save(direrr)
    }

    // process all other lines
    val cnx = cnx_all //.filter($"wr_filedt" !== null)
    val all_dt = cnx.select($"dt").distinct().collect().map(_.getString(0))

    //Read and resolve
    val cnx_filtered = cnx_parquet.filter($"dt".isin(all_dt:_*))
      .join(cnx, cnx_parquet("collecttype") === cnx("type") && cnx_parquet("engine") === cnx("engine")
        && cnx_parquet("filedt") === cnx("filedt") && cnx_parquet("dt") === cnx("dt"), "inner")
      .select(cnx_parquet.columns.map(cnx_parquet.col):_*)

    val wr_filtered = wr.filter($"dt".isin(all_dt:_*))
      .join(cnx, wr("collecttype") === cnx("type") && wr("engine") === cnx("engine")
        && wr("filedt") === cnx("filedt") && wr("dt") === cnx("dt"), "inner")
      .select(wr.columns.map(wr.col):_*)

    println("pipeline3to4() : data resolutions")
    val cnx_resolved = resolveAIP(resolveSector(resolveSite(cnx_filtered)))

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
        |select c.*, w.url, $jobidcur as jobid, $jobidorig as jobidorig
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
      .partitionBy("collecttype", "dt", "engine", "filedt", "jobidorig", "jobid").parquet(dirout)
  }

  def buildIpLookupTable(): DataFrame = {
    val ip = repo.readMDM().select($"mdm_loc_code", explode(range($"mdm_ip_start_int", $"mdm_ip_end_int")).as("mdm_ip"))
    ip.registerTempTable("ip")
    broadcast(sqlContext.sql("""select mdm_ip, concat_ws('_',collect_set(mdm_loc_code)) as site from ip group by mdm_ip"""))
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
    val dfI_ID = repo.readI_ID().select("I_ID","Sector","SiteCode")

    //join for resolution
    df.join(broadcast(dfI_ID).as("dfID"), df("I_ID_U") === dfI_ID("I_ID"), "left_outer")
      .select((df.columns.toList.map(df.col)
        ++ List($"dfID.Sector".as("source_sector"),formatSite($"dfID.SiteCode").as("source_I_ID_site"))):_*)
  }

  def resolveAIP(df: DataFrame): DataFrame = {

    val dfAIP = broadcast(repo.readAIP().persist(StorageLevel.MEMORY_AND_DISK))

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
