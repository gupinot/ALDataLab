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
    val metaDf23 = aggregateMeta(loadMeta(context.meta()), Pipeline2To3.STAGE_NAME).as("metaDf23")
    val metaDf34 = aggregateMeta(loadMeta(context.meta()), Pipeline3To4.STAGE_NAME).as("metaDf34")

    //keep only connection delta (meta23 not in meta34)
    val cnx_meta_delta = metaDf23.join(metaDf34,
      ($"metaDf23.dt" === $"metaDf34.dt") and ($"metaDf23.engine" === $"metaDf34.engine")
      and ($"metaDf23.collecttype" === $"metaDf34.collecttype") and ($"metaDf23.filetype" === $"metaDf34.filetype")
      and ($"metaDf23.filetype" === "connection") and ($"metaDf23.min_filedt" === $"metaDf34.min_filedt"),
      "left_outer")
      .filter("metaDf34.dt is null")
      .select(metaDf23.columns.map(metaDf23.col):_*).as("cnx_meta_delta")

    //select only record from cnx_meta_delta with corresponding webrequest record in metaDf23
    val cnx_meta_delta_ok = cnx_meta_delta.join(metaDf23.filter($"filetype" === "webrequest"),
      $"cnx_meta_delta.engine" === $"metaDf23.engine"
      and $"cnx_meta_delta.min_filedt" === $"metaDf23.min_filedt",
      "inner")
      .select(cnx_meta_delta.columns.map(cnx_meta_delta.col):_*)

    val cnx = broadcast(cnx_meta_delta_ok)

    val all_dt = cnx.select($"dt").distinct().collect().map(_.getDate(0))

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
    val cnx_resolved = resolveSite(resolveAIP(resolveSector((cnx_filtered))))

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
      .partitionBy("dt").parquet(context.dirout())

    val cnx_meta_result = cnx_meta_delta_ok.withColumn("Stage", lit(Pipeline3To4.STAGE_NAME))
    cnx_meta_result.write.mode(SaveMode.Append).parquet(context.meta())

    // save job results to control
    cnx_meta_result.withColumn("jobid",lit(jobidcur.toString)).withColumn("status",lit("OK"))
      .write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter",";")
      .save(s"${context.control()}/$jobidcur.csv")

  }

  def buildIpLookupTable(): DataFrame = {
    val ip = context.repo().readMDM().select($"mdm_loc_code", explode(range($"mdm_ip_start_int", $"mdm_ip_end_int")).as("mdm_ip"))
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
    val dfI_ID = context.repo().readI_ID().select("I_ID","Sector","SiteCode").cache()

    //join for resolution
    df.join(dfI_ID.as("dfID"), df("I_ID_U") === dfI_ID("I_ID"), "left_outer")
      .select((df.columns.toList.map(df.col)
        ++ List($"dfID.Sector".as("source_sector"),formatSite($"dfID.SiteCode").as("source_I_ID_site"))):_*)
  }

  def resolveAIP(df: DataFrame): DataFrame = {

    val dfAIP = context.repo().readAIP().cache()

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
