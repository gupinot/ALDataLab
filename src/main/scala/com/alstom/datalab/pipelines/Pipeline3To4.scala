package com.alstom.datalab.pipelines

import com.alstom.datalab.exception.MissingWebRequestException
import com.alstom.datalab.{Pipeline, Repo}
import com.alstom.datalab.Util._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by guillaumepinot on 05/11/2015.
  */
class Pipeline3To4(sqlContext: SQLContext) extends Pipeline {
  import sqlContext.implicits._

  var AIPToResolve = true

  def execute(): Unit = {
    this.inputFiles.foreach((filein)=> {
      val filename = new Path(filein).getName
      val Array(filetype, fileenginename, filedate) = filename.replaceAll("\\.[^_]+$","").split("_")
      val webrequest_filename = filein.replaceFirst("connection", "webrequest")
      val fileout = s"$dirout/$filename"

      if (filetype == "webrequest") {
        println("pipeline3to4() : webrequest file : nothing to do (will be compute with connection file)")
        return
      }

      println("pipeline3to4() : read filein : " + filein)
      val df = sqlContext.read.parquet(filein).cache()

      println("pipeline3to4() : SiteResolution")
      val dfSite = SiteResolutionFromIP(df)

      println("pipeline3to4() : Sector resolution")
      val dfSiteSector = SiteAndSectorResolutionFromI_ID(dfSite)

      //AIP resolution
      println("pipeline3to4() : AIP resolution")
      val dfResolved = if (AIPToResolve) AIPResolution(dfSiteSector) else dfSiteSector
      dfResolved.registerTempTable("connections")

      //Web request resolution
      println("pipeline3to4() : Web request join")
      val dfWebRequest = try {
        sqlContext.read.parquet(webrequest_filename).cache()
      } catch {
        case e: java.lang.AssertionError => println("pipeline3to4() : ERR : webrequest file (" + webrequest_filename + ") not found")
          throw new MissingWebRequestException(webrequest_filename)
      }
      dfWebRequest.registerTempTable("webrequest")

      //join with df
      val dfres = sqlContext.sql(
        """
          |with weburl as (
          |   select I_ID_D,source_app_name,dest_ip,dest_port,enddate,concat_ws('|',collect_set(url)) as url
          |   from webrequest
          |   group by I_ID_D,source_app_name,dest_ip,dest_port,enddate
          |)
          |select c.*,w.url
          |from connections c
          |left outer join weburl w on (
          |  c.I_ID_D=w.I_ID_D
          |  and c.source_app_name=w.source_app_name
          |  and c.dest_port=w.dest_port
          |  and c.enddate = w.enddate
          |)
        """.stripMargin)

      println("pipeline3to4() : write result : " + fileout)
      dfres.write.mode("overwrite").parquet(fileout)
    })
  }

  def SiteResolutionFromIP(df: DataFrame): DataFrame = {

    //Read MDM repository file
    val dfMDM = repo.readMDM().select("mdm_loc_code", "mdm_ip_start_int", "mdm_ip_end_int").cache()

    //extract IP to resolve
    val dfIpInt = df
      .withColumn("dest_ip_int", ip2Long($"dest_ip"))
      .withColumn("source_ip_int", ip2Long($"source_ip"))
      .cache()

    val ListIPToResolve = dfIpInt
      .select("dest_ip_int").withColumnRenamed("dest_ip_int", "IP")
      .unionAll(dfIpInt.select("source_ip_int").withColumnRenamed("source_ip_int", "IP")).distinct
    ListIPToResolve.cache()

    //Resolve Site
    val dfSiteResolved = ListIPToResolve.join(dfMDM, ListIPToResolve("IP") >= dfMDM("mdm_ip_start_int") && ListIPToResolve("IP") <= dfMDM("mdm_ip_end_int"))
      .drop("mdm_ip_start_int")
      .drop("mdm_ip_end_int").distinct

    val myConcat = new ConcatString("_")

    val dfSiteResolvedConcat = dfSiteResolved.groupBy($"IP").agg(myConcat($"mdm_loc_code").as("site")).withColumnRenamed("IP", "IP2")

    val ListIPResolved = ListIPToResolve.join(dfSiteResolvedConcat, ListIPToResolve("IP") ===  dfSiteResolvedConcat("IP2"), "left_outer")
      .select("IP", "site")
      .withColumn("site_", formatSite($"site")).drop("site").withColumnRenamed("site_", "site")
    ListIPResolved.cache()

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
    dfI_ID.cache()

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

    val dfAIP = repo.readAIP().cache()

    val collecttype_ = df.select("collecttype").distinct.collect.flatMap(_.toSeq).map(_.toString())
    val collecttype = collecttype_(0)

    val dfres1 = {
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

      df1.join(dfAIP,
        df1("dest_ip") === dfAIP("aip_server_ip"), "left_outer")
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
    }

    if (collecttype == "server") {
      // drop result columns from df (source_aip...) if already exist
      val df2: DataFrame = {
        if (dfres1.columns.contains("source_aip_server_adminby")) {
          dfres1.drop("source_aip_server_adminby")
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
        } else dfres1
      }

      df2.join(dfAIP,
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
    }else {
      dfres1
    }
  }
}
