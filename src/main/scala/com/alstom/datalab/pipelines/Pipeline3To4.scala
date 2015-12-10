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

    val dfwr = sqlContext.read.option("mergeSchema", "false").parquet(s"$dirin/webrequest/")
    dfwr.persist(StorageLevel.MEMORY_AND_DISK).registerTempTable("webrequest")

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

  def buildIpLookupTable(): DataFrame = {
    val ip = repo.readMDM().select($"mdm_loc_code", explode(range($"mdm_ip_start_int", $"mdm_ip_end_int")).as("mdm_ip"))
    ip.registerTempTable("ip")
    broadcast(sqlContext.sql("""select mdm_ip, concat_ws('_',collect_set(mdm_loc_code)) as site from ip group by mdm_ip"""))
  }

  def SiteResolutionFromIP(df: DataFrame): DataFrame = {

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

  def SiteAndSectorResolutionFromI_ID(df: DataFrame): DataFrame = {
    val dfI_ID = repo.readI_ID().select("I_ID","Sector","SiteCode")

    //join for resolution
    df.join(broadcast(dfI_ID).as("dfID"), df("I_ID_U") === dfI_ID("I_ID"), "left_outer")
      .select((df.columns.toList.map(df.col)
        ++ List($"dfID.Sector".as("source_sector"),formatSite($"dfID.SideCode").as("source_I_ID_site"))):_*)
  }

  def AIPResolution(df: DataFrame): DataFrame = {

    val dfAIP = broadcast(repo.readAIP().persist(StorageLevel.MEMORY_AND_DISK))

    df.join(dfAIP.as("destAip"), $"df.dest_ip" === $"destAip.aip_server_ip", "left_outer")
      .join(dfAIP.as("sourceAip"), $"df.source_ip" === $"sourceAip.aip_server_ip", "left_outer")
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
