package com.alstom.datalab

import com.alstom.datalab.Util._
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions._
import org.joda.time.format._



/**
  * Created by guillaumepinot on 05/11/2015.
  */

class Pipeline(repo: Repo)(implicit val sc: SparkContext, implicit val sqlContext: SQLContext) {
  import sqlContext.implicits._

  /*
  Arbo : a file is set done by writting file.done file
    2 : csv file (connection or webrequest type)
    finale :
      enddate
        engine
          source_sector
            source_site
              dest_site

 */

  def pipeline2to3(filein: String, dirout: String): Boolean = {

    val filename = new Path(filein).getName
    val Array(filetype,fileenginename,filedate) = filename.substring(0,filename.lastIndexOf('.')).split("_")

    println("pipeline2to3() : read filein")
    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema","true")
      .option("mode", "DROPMALFORMED")
      //.option("parserLib", "UNIVOCITY")
      .load(filein)

    //rename columns
    println("pipeline2to3() : rename column")
    val df2 = {
      if (filetype == "connection") {
        df.withColumnRenamed("NX_con_start_time", "start_time")
          .withColumnRenamed("NX_con_end_time", "end_time")
          .withColumnRenamed("NX_con_duration", "con_duration")
          .withColumnRenamed("NX_con_cardinality", "con_number")
          .withColumnRenamed("NX_con_destination_ip", "dest_ip")
          .withColumnRenamed("NX_con_out_traffic", "con_traffic_out")
          .withColumnRenamed("NX_con_in_traffic", "con_traffic_in")
          .withColumnRenamed("NX_con_type", "con_protocol")
          .withColumnRenamed("NX_con_status", "con_status")
          .withColumnRenamed("NX_con_port", "dest_port")
          .withColumnRenamed("NX_bin_app_category", "source_app_category")
          .withColumnRenamed("NX_bin_app_company", "source_app_company")
          .withColumnRenamed("NX_bin_app_name", "source_app_name")
          .withColumnRenamed("NX_bin_exec_name", "source_app_exec")
          .withColumnRenamed("NX_bin_paths", "source_app_paths")
          .withColumnRenamed("NX_bin_version", "source_app_version")
          .withColumnRenamed("NX_device_last_ip", "source_ip")
      }
      else
      {
        df.withColumnRenamed("wr_start_time", "start_time")
          .withColumnRenamed("wr_end_time", "end_time")
          .withColumnRenamed("wr_url", "url")
          .withColumnRenamed("wr_destination_port", "dest_port")
          .withColumnRenamed("wr_destination_ip", "dest_ip")
          .withColumnRenamed("wr_application_name", "source_app_name")
      }
    }

    //split start_time and end_time into 2 fields date and time
    println("pipeline2to3() : split start_time and end_time into 2 fields date and time")
    val resdf = df2
      .withColumn("startdate", to_date($"start_time"))
      .withColumn("starttime", hour($"start_time"))
      .withColumn("enddate", to_date($"end_time"))
      .withColumn("endtime", hour($"end_time"))
      .withColumn("engine", lit(fileenginename))
      .withColumn("fileenginedate", lit(filedate))
      .withColumn("collecttype", getCollectType($"engine"))
      .drop("start_time").drop("end_time")

    resdf.registerTempTable("df")

    //split resdf by enddate
    println("pipeline2to3() : split resdf by enddate")

    val dfnotdone = sc.parallelize(List("false")).toDF("notdone")

    val days = sqlContext.sql(
      """
        |select enddate,max(endtime) as max_endtime,min(endtime) as min_endtime
        |from df
        |group by endate
        |having max(endtime) >= 23 and min(endtime) <= 3
      """.stripMargin)

    days.foreach( row => {
      val day = row.getDate(0)
      val fileout = s"$dirout/${filetype}_${fileenginename}_$day.parquet"

      try {
        resdf.where($"enddate" === day).write.parquet(fileout)
        println("pipeline2to3() : No duplicated")
        dfnotdone.write.mode("overwrite").parquet(fileout+".todo")
      } catch {
        case e: Exception => {
          println("pipeline2to3() : Duplicated")
        }
      }
      println(s"pipeline2to3() : CompleteDay $day")
    })
    true
  }

  def pipeline3to4(filein: String, dirout: String): Boolean = {
    pipeline3to4(filein, dirout, true)
  }

  def pipeline3to4(filein: String, dirout: String, AIPToResolve: Boolean): Boolean = {

    val filename = new Path(filein).getName()
    val filetype = filename.replaceFirst("_.*", "")
    val fileenginename = filename.replaceFirst(filetype+"_", "").replaceFirst("_.*", "")
    val fileday=filename.replaceFirst(filetype+"_", "").replaceFirst(fileenginename+"_", "").replaceFirst("\\.parquet$", "")

    val fileout = dirout + "/" + filename

    if (filetype == "webrequest") {
      println("pipeline3to4() : webrequest file : nothing to do (will be compute with connection file)")
      return true
    }

    val dfnotdone = sc.parallelize(List("false")).toDF("notdone")

    println("pipeline3to4() : read filein : " + filein)
    val df = sqlContext.read.parquet(filein)

    println("pipeline3to4() : SiteResolution")
    val dfSite = SiteResolutionFromIP(df)

    println("pipeline3to4() : Sector resolution")
    val dfSiteSector = SiteAndSectorResolutionFromI_ID(dfSite)

    //Web request resolution
    println("pipeline3to4() : Web request resolution")
    val webrequest_filename = filein.replaceFirst("connection", "webrequest")
    val dfSiteSector4WebRequest = {
      try {
        val dfWebRequest = sqlContext.read.parquet(webrequest_filename)
        WebRequestResolution(dfSiteSector, dfWebRequest)
      } catch {
        case e: java.lang.AssertionError => println("pipeline3to4() : ERR : webrequest file (" + webrequest_filename + ") not found")
          null
        case _:Throwable => null
      }
    }
    if (dfSiteSector4WebRequest == null) {
      return false
    }

    //AIP resolution
    println("pipeline3to4() : AIP resolution")
    val dfSiteSector4WebRequestAIP = {
      if (AIPToResolve) {
        AIPResolution(dfSiteSector4WebRequest)
      }else {
        dfSiteSector4WebRequest
      }
    }

    println("pipeline3to4() : write result : " + fileout)
    dfSiteSector4WebRequestAIP.write.mode("overwrite").parquet(fileout)
    dfnotdone.write.mode("overwrite").parquet(fileout+".todo")

    true
  }

  def pipeline4to5(filein: String, diroutByPartition: String): Boolean = {

    val df = sqlContext.read.parquet(filein)
    df.write.mode("append").partitionBy("collecttype", "enddate", "engine", "source_sector").parquet(diroutByPartition)
    true
  }

  def SiteResolutionFromIP(df: DataFrame): DataFrame = {

    //Read MDM repository file
    val dfMDM = repo.readMDM()
      .select("mdm_loc_code", "mdm_ip_start_int", "mdm_ip_end_int")

    dfMDM.cache()

    //extract IP to resolve
    val dfIpInt = df
      .withColumn("dest_ip_int", ip2Long($"dest_ip"))
      .withColumn("source_ip_int", ip2Long($"source_ip"))
    dfIpInt.cache()

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
    val df4IPSite = dfIpIntClean
      .join(ListIPResolved, dfIpIntClean("source_ip_int") === ListIPResolved("IP"), "left_outer")
      .withColumnRenamed("site", "source_site").drop("IP")
      .join(ListIPResolved, dfIpIntClean("dest_ip_int") === ListIPResolved("IP"), "left_outer")
      .withColumnRenamed("site", "dest_site").drop("IP")
      .drop("source_ip_int").drop("dest_ip_int")

    return df4IPSite
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
    val dfIID = df1.join(dfI_ID, df("I_ID_U") === dfI_ID("I_ID2"), "left_outer")
      .drop("I_ID2")
      .withColumnRenamed("SiteCode", "source_I_ID_site")
      .withColumn("source_I_ID_site", formatSite($"source_I_ID_site"))

    return dfIID

  }

  def AIPResolution(df: DataFrame): DataFrame = {

    val dfAIP = repo.readAIP()
    dfAIP.cache()

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

    val dfres2 = {
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

    return dfres2

  }

  def WebRequestResolution(df: DataFrame, dfWebRequest: DataFrame): DataFrame = {

    val myConcat = new ConcatString("|")

    val df4Web = dfWebRequest
      .select("I_ID_D", "source_app_name", "dest_ip", "dest_port", "enddate", "url")
      .distinct
      .groupBy("I_ID_D", "source_app_name", "dest_ip", "dest_port", "enddate").agg(myConcat($"url").as("url"))
      .withColumnRenamed("I_ID_D", "wr_I_ID_D")
      .withColumnRenamed("source_app_name", "wr_source_app_name")
      .withColumnRenamed("dest_ip", "wr_dest_ip")
      .withColumnRenamed("dest_port", "wr_dest_port")
      .withColumnRenamed("enddate", "wr_enddate")

    //join with df
    val dfres = df
      .join(df4Web,
        df("I_ID_D") === df4Web("wr_I_ID_D")
          && df("source_app_name") === df4Web("wr_source_app_name")
          && df("dest_ip") === df4Web("wr_dest_ip")
          && df("dest_port") === df4Web("wr_dest_port")
          && df("enddate") === df4Web("wr_enddate"),
        "left_outer")
      .drop("wr_I_ID_D")
      .drop("wr_source_app_name")
      .drop("wr_dest_ip")
      .drop("wr_dest_port")
      .drop("wr_enddate")

    return dfres
  }
}
