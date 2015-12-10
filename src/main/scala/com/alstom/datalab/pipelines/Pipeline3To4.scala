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

    //read control files from inputFiles and filter on connection filetype)
    val dfControl: DataFrame = this.inputFiles.map(filein => {sc.textFile(filein)})
      .reduce(_.union(_))
      .map(_.split(";"))
      .map(s => ControlFile(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7)))
      .toDF()
    val jobidorig=dfControl.select("jobid").distinct().head.getString(0)

    val dfControlConnection = dfControl
      .filter($"filetype" === "connection")
      .select($"collecttype" as "ctl_collecttype",
        $"engine" as "ctl_engine",
        to_date($"day") as "ctl_day",
        to_date($"filedt") as "ctl_filedt")

    val dfdirinconnection = sqlContext.read.option("mergeSchema", "false").parquet(s"$dirin/connection/")
    dfdirinconnection.persist(StorageLevel.MEMORY_AND_DISK)

    //select correct filedt from dfdriin
    val dfdirinconnectionCorrect = dfdirinconnection
        //.select("filedt","collecttype","dt","engine")
        .groupBy("collecttype", "dt", "engine")
        .agg(min(to_date($"filedt")) as "minfiledt")

    //select correct filedt from dfControlConnection
    val dfControlConnectionToDo = dfdirinconnectionCorrect.join(dfControlConnection,
      dfdirinconnectionCorrect("collecttype") === dfControlConnection("ctl_collecttype")
      && dfdirinconnectionCorrect("engine") === dfControlConnection("ctl_engine")
      && dfdirinconnectionCorrect("dt") === dfControlConnection("ctl_day")
      && dfdirinconnectionCorrect("minfiledt") === dfControlConnection("ctl_filedt"),
      "inner")

    dfControlConnectionToDo.persist(StorageLevel.MEMORY_AND_DISK)

    // load corresponding webrequest
    val dfwr = sqlContext.read.option("mergeSchema", "true").parquet(s"$dirin/webrequest/")
    dfwr.persist(StorageLevel.MEMORY_AND_DISK)


    //verify existence of all webrequest files corresponding to connection files in dfControlConnectionToDo
    val dfwrRenammed = dfwr
      .select("collecttype", "engine", "filedt", "dt")
      .withColumnRenamed("collecttype", "wrcollecttype")
      .withColumnRenamed("engine", "wrengine")
      .withColumnRenamed("filedt", "wrfiledt")
      .withColumnRenamed("dt", "wrdt")

    val dfControlConnectionToDOWithWR = dfControlConnectionToDo.join(dfwrRenammed,
      dfControlConnectionToDo("collecttype") === dfwrRenammed("wrcollecttype")
        && dfControlConnectionToDo("engine") === dfwrRenammed("wrengine")
        && dfControlConnectionToDo("minfiledt") === dfwrRenammed("wrfiledt")
        && dfControlConnectionToDo("dt") === dfwrRenammed("wrdt"),
      "left_outer")
    val dfwrnotexists =  dfControlConnectionToDOWithWR.filter($"wrfiledt" === null)
    val dfControlConnectionToDoFinal = {
      if (dfwrnotexists .count() != 0) {
        println("following webrequest files not found :")
        dfwrnotexists.select("collecttype", "engine", "minfiledt", "dt").withColumn("Status", lit("webrequest not found")).write.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", ";").save(direrr)
        dfControlConnectionToDOWithWR.filter($"wrfiledt" !== null)
      }else {
        dfControlConnectionToDOWithWR
      }
    }

    //Read and resolve
    val dfdirinconnection2 = dfdirinconnection
      .join(dfControlConnectionToDoFinal, dfdirinconnection("collecttype") === dfControlConnectionToDoFinal("collecttype")
        && dfdirinconnection("engine") === dfControlConnectionToDoFinal("engine")
        && dfdirinconnection("filedt") === dfControlConnectionToDoFinal("minfiledt")
        && dfdirinconnection("dt") === dfControlConnectionToDoFinal("dt"),
        "inner")
      .select(dfdirinconnection.columns.map(dfdirinconnection.col):_*)

      println("pipeline3to4() : SiteResolution")
      val dfSite = SiteResolutionFromIP(dfdirinconnection2)

      println("pipeline3to4() : Sector resolution")
      val dfSiteSector = SiteAndSectorResolutionFromI_ID(dfSite)

      //AIP resolution
      println("pipeline3to4() : AIP resolution")
      val dfResolved = AIPResolution(dfSiteSector)
      dfResolved.registerTempTable("connections")

      dfwr.registerTempTable("webrequest")

      //join with df
      val dfres = sqlContext.sql(
        """
          |with weburl as (
          |   select collecttype, dt, engine, filedt, I_ID_D,source_app_name,dest_ip,dest_port,concat_ws('|',collect_set(url)) as url
          |   from webrequest
          |   group by collecttype, dt, engine, filedt, I_ID_D,source_app_name,dest_ip,dest_port
          |)
          |select c.*,w.url
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

    dfres.withColumn("jobid", lit(jobidcur)).withColumn("jobidorig", lit(jobidorig))
      .write.mode(SaveMode.Append).partitionBy("collecttype", "dt", "engine", "filedt", "jobidorig", "jobid").parquet(dirout)
  }

  def SiteResolutionFromIP(df: DataFrame): DataFrame = {

    //Read MDM repository file
    val dfMDM = repo.readMDM().select("mdm_loc_code", "mdm_ip_start_int", "mdm_ip_end_int").persist(StorageLevel.MEMORY_AND_DISK)

    //extract IP to resolve
    val dfIpInt = df
      .withColumn("dest_ip_int", ip2Long($"dest_ip"))
      .withColumn("source_ip_int", ip2Long($"source_ip"))

    val ListIPToResolve = dfIpInt
      .select("dest_ip_int").withColumnRenamed("dest_ip_int", "IP")
      .unionAll(dfIpInt.select("source_ip_int").withColumnRenamed("source_ip_int", "IP")).distinct


    //Resolve Site
    val dfSiteResolved = ListIPToResolve.join(dfMDM, ListIPToResolve("IP") >= dfMDM("mdm_ip_start_int") && ListIPToResolve("IP") <= dfMDM("mdm_ip_end_int"))
      .drop("mdm_ip_start_int")
      .drop("mdm_ip_end_int").distinct

    val myConcat = new ConcatString("_")

    val dfSiteResolvedConcat = dfSiteResolved.groupBy($"IP").agg(myConcat($"mdm_loc_code").as("site")).withColumnRenamed("IP", "IP2")

    val ListIPResolved = ListIPToResolve.join(dfSiteResolvedConcat, ListIPToResolve("IP") ===  dfSiteResolvedConcat("IP2"), "left_outer")
      .select("IP", "site")
      .withColumn("site_", formatSite($"site")).drop("site").withColumnRenamed("site_", "site")


    //suppress result column (source_site, dest_site) in dfIpInt if already exist
    val dfIpIntClean: DataFrame = {
      val dftmp = {
        if (dfIpInt.columns.contains("source_site")) {
          dfIpInt.drop("source_site")
        } else dfIpInt
      }
      if (dfIpInt.columns.contains("dest_site")) {
        dftmp .drop("dest_site")
      } else dftmp
    }

    //join with dfIpIntClean
    dfIpIntClean
      .join(ListIPResolved, dfIpIntClean("source_ip_int") === ListIPResolved("IP"), "left_outer")
      .withColumnRenamed("site", "source_site").drop("IP")
      .join(ListIPResolved, dfIpIntClean("dest_ip_int") === ListIPResolved("IP"), "left_outer")
      .withColumnRenamed("site", "dest_site").drop("IP")
      .drop("source_ip_int").drop("dest_ip_int")
  }

  def SiteAndSectorResolutionFromI_ID(df: DataFrame): DataFrame = {
    val dfI_ID = repo.readI_ID()
      .withColumnRenamed("I_ID", "I_ID2")
      .withColumnRenamed("Sector", "source_sector")
      .withColumnRenamed("siteCode", "source_I_ID_site")

    // drop result columns from df (Source_sector, source_I_ID_site) if already exist
    val df1: DataFrame = {
      val dftmp = {
        if (df.columns.contains("source_sector")) {
          df.drop("source_sector")
        } else df
      }
      if (df.columns.contains("source_I_ID_site")) {
        dftmp .drop("source_I_ID_site")
      } else dftmp
    }

    //join for resolution
    df1.join(dfI_ID, df("I_ID_U") === dfI_ID("I_ID2"), "left_outer")
      .drop("I_ID2")
      .withColumnRenamed("SiteCode", "source_I_ID_site")
      .withColumn("source_I_ID_site", formatSite($"source_I_ID_site"))
  }

  def AIPResolution(df: DataFrame): DataFrame = {

    val dfAIP = repo.readAIP().persist(StorageLevel.MEMORY_AND_DISK)

      // drop result columns from df (dest_aip...) if already exist
      val df1: DataFrame = {
        if (df.columns.contains("dest_aip_server_adminby")) {
          df.drop("dest_aip_server_adminby")
            .drop("dest_aip_server_function")
            .drop("dest_aip_server_subfunction")
            .drop("dest_aip_app_name")
            .drop("dest_aip_app_type")
            .drop("dest_aip_appinstance_type")
            .drop("dest_aip_app_state")
            .drop("dest_aip_app_sensitive")
            .drop("dest_aip_app_criticality")
            .drop("dest_aip_app_sector")
            .drop("dest_aip_appinstance_shared_unique_id")
        } else df
      }

      val df2: DataFrame = {
        if (df1.columns.contains("source_aip_server_adminby")) {
          df1.drop("source_aip_server_adminby")
            .drop("source_aip_server_function")
            .drop("source_aip_server_subfunction")
            .drop("source_aip_app_name")
            .drop("source_aip_app_type")
            .drop("source_aip_appinstance_type")
            .drop("source_aip_app_state")
            .drop("source_aip_app_sensitive")
            .drop("source_aip_app_criticality")
            .drop("source_aip_app_sector")
            .drop("source_aip_appinstance_shared_unique_id")
        } else df1
      }


      df2.join(dfAIP,
        df2("dest_ip") === dfAIP("aip_server_ip"), "left_outer")
        .drop("aip_server_ip")
        .withColumnRenamed("aip_server_adminby", "dest_aip_server_adminby")
        .withColumnRenamed("aip_server_function", "dest_aip_server_function")
        .withColumnRenamed("aip_server_subfunction", "dest_aip_server_subfunction")
        .withColumnRenamed("aip_app_name", "dest_aip_app_name")
        .withColumnRenamed("aip_app_type", "dest_aip_app_type")
        .withColumnRenamed("aip_appinstance_type", "dest_aip_appinstance_type")
        .withColumnRenamed("aip_app_state", "dest_aip_app_state")
        .withColumnRenamed("aip_app_sensitive", "dest_aip_app_sensitive")
        .withColumnRenamed("aip_app_criticality", "dest_aip_app_criticality")
        .withColumnRenamed("aip_app_sector", "dest_aip_app_sector")
        .withColumnRenamed("aip_app_shared_unique_id", "dest_aip_app_shared_unique_id")
        .join(dfAIP,
          df2("source_ip") === dfAIP("aip_server_ip"), "left_outer")
        .drop("aip_server_ip")
        .withColumnRenamed("aip_server_adminby", "source_aip_server_adminby")
        .withColumnRenamed("aip_server_function", "source_aip_server_function")
        .withColumnRenamed("aip_server_subfunction", "source_aip_server_subfunction")
        .withColumnRenamed("aip_app_name", "source_aip_app_name")
        .withColumnRenamed("aip_app_type", "source_aip_app_type")
        .withColumnRenamed("aip_appinstance_type", "source_aip_appinstance_type")
        .withColumnRenamed("aip_app_state", "source_aip_app_state")
        .withColumnRenamed("aip_app_sensitive", "source_aip_app_sensitive")
        .withColumnRenamed("aip_app_criticality", "source_aip_app_criticality")
        .withColumnRenamed("aip_app_sector", "source_aip_app_sector")
        .withColumnRenamed("aip_app_shared_unique_id", "source_aip_app_shared_unique_id")
    }
}
