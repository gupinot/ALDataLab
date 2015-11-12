package dlpipeline

import dlenv._
import dlutil._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.csv._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.joda.time.format._
import org.joda.time._



/**
  * Created by guillaumepinot on 05/11/2015.
  */

class dlpipeline {
  def pipeline2to3(filein: String): Boolean = {

    val filetype = filein.replaceFirst("_.*", "")
    val fileenginename = filein.replaceFirst(filetype+"_", "").replaceFirst("_.*", "")
    val filedate=filein.replaceFirst(filetype+"_", "").replaceFirst(fileenginename+"_", "").replaceFirst("\\..*", "")

    val sparkenv = new spark_env
    val sqlContext = sparkenv.init()
    import sqlContext.implicits._

    println("DLPipeline() : read filein")
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter",";").option("inferSchema","true").load(D_NX2 + "/in/" + filein)
    df.cache()

    //rename columns
    println("DLPipeline() : rename column")
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
    println("DLPipeline() : split start_time and end_time into 2 fields date and time")
    val resdf = df2.withColumn("startdate", dlutil.getFirst(daypattern)($"start_time"))
      .withColumn("starttime", dlutil.getFirst(timepattern)($"start_time"))
      .withColumn("enddate", dlutil.getFirst(daypattern)($"end_time"))
      .withColumn("endtime", dlutil.getFirst(timepattern)($"end_time"))
      .drop("start_time").drop("end_time")

    //split resdf by enddate
    println("DLPipeline() : split resdf by enddate")
    val daylist = resdf.select("enddate").distinct.collect.flatMap(_.toSeq)
    val dfbydaylistArray = daylist.map(day => resdf.where($"enddate" <=> day))

    //store each df in array in parquet file and move source file in Archives directory
    val hadoopConfig = new hadoop_env
    val fs = hadoopConfig.init()


    if (!fs.exists(new Path(D_NX2 + "/Done"))) {
      println("Create " + D_NX2 + "/Done directory")
      fs.mkdirs(new Path(D_NX2 + "/Done"))
    }

    if (!fs.exists(new Path(D_NX3))) {
      println("Create " + D_NX3)
      fs.mkdirs(new Path(D_NX3))
    }

    if (!fs.exists(new Path(D_NX3 + "/in"))) {
      println("Create " + D_NX3 + "/in")
      fs.mkdirs(new Path(D_NX3 + "/in"))
    }

    println("DLPipeline() : write parquet file")
    dfbydaylistArray.map(
      dfday => {
        val day = dfday.select("enddate").distinct.collect.flatMap(_.toSeq)
        val parquetfilename = D_NX3+"/in/"+filetype+"_"+fileenginename+"_"+filedate+"_"+day(0).toString()+".parquet"
        dfday.write.format("parquet").mode("overwrite").save(parquetfilename)
      }
    )

    //move filename to Done
    println("DLPipeline() : move filename to Done")
    val res = fs.rename(new Path(D_NX2+"/in/"+filein), new Path(D_NX2+"/Done/"+filein))

    return true
  }

  def pipeline3to4(filein: String): Boolean = {


    val JodaTimeFormatter = DateTimeFormat.forPattern("HH:mm:ss");

    val hadoopConfig = new hadoop_env
    val fs = hadoopConfig.init()

    if (!fs.exists(new Path(D_NX4))) {
      println("Create " + D_NX4)
      fs.mkdirs(new Path(D_NX4))
    }
    if (!fs.exists(new Path(D_NX4+"/in"))) {
      println("Create " + D_NX4+"/in")
      fs.mkdirs(new Path(D_NX4+"/in"))
    }
    if (!fs.exists(new Path(D_NX3+"/Done"))) {
      println("Create " + D_NX3+"/Done")
      fs.mkdirs(new Path(D_NX3+"/Done"))
    }
    if (!fs.exists(new Path(D_NX3+"/NoCompleteDay"))) {
      println("Create " + D_NX3+"/NoCompleteDay")
      fs.mkdirs(new Path(D_NX3+"/NoCompleteDay"))
    }

    val filetype = filein.replaceFirst("_.*", "")
    val fileenginename = filein.replaceFirst(filetype+"_", "").replaceFirst("_.*", "")
    val filedate=filein.replaceFirst(filetype+"_", "").replaceFirst(fileenginename+"_", "").replaceFirst("_.*", "")
    val fileday=filein.replaceFirst(filetype+"_", "").replaceFirst(fileenginename+"_", "").replaceFirst(filedate+"_", "").replaceFirst("\\.parquet$", "")

    val sparkenv = new spark_env
    val sqlContext = sparkenv.init()
    import sqlContext.implicits._

    println("pipeline3to4() : read parquet file. filein : " + filein)
    val df = sqlContext.read.parquet(D_NX3 +"/in/" + filein)
    df.cache()

    val dfDay = df.agg(min((df("endtime"))), max(df("endtime")))
    //val dfDay = df.agg((min(df("endtime"))), max(df("endtime")))
    val minendtime = dfDay.select("min(endtime)").collect.flatMap(_.toSeq)
    val maxendtime = dfDay.select("max(endtime)").collect.flatMap(_.toSeq)

    if (JodaTimeFormatter.parseDateTime(minendtime(0).toString()).isBefore(JodaTimeFormatter.parseDateTime("03:00:00")) && JodaTimeFormatter.parseDateTime(maxendtime(0).toString()).isAfter(JodaTimeFormatter.parseDateTime("23:00:00"))) {
      println("pipeline3to4() : CompleteDay")
      //is file with same engine name and same day already exists ?
      if (fs.globStatus(new Path(D_NX4+"/in/"+filetype+"_"+fileenginename+"_*_"+fileday+".parquet")).length == 0 && fs.globStatus(new Path(D_NX4+"/Done/"+filetype+"_"+fileenginename+"_*_"+fileday+".parquet")).length == 0) {
        //no duplicated (file not already exists) => move to /DATA/4-NXFile/in
        println("pipeline3to4() : No duplicated")
        df.write.format("parquet").mode("overwrite").save(D_NX3+"/Done/"+filein)
        fs.rename(new Path(D_NX3+"/in/"+filein), new Path(D_NX4+"/in/"+filein))
      }else {
        // duplicated (file already exists) => delete file input
        println("pipeline3to4() : Duplicated")
        fs.delete(new Path(D_NX3+"/in/"+filein))
      }
    }else {
      println("pipeline3to4() : no CompleteDay")
      val res = fs.rename(new Path(D_NX3+"/in/"+filein), new Path(D_NX3+"/NoCompleteDay/"+filein))
    }

    return true
  }

  def pipeline4to5(filein: String): Boolean = {
    val hadoopConfig = new hadoop_env
    val fs = hadoopConfig.init()

    if (!fs.exists(new Path(D_NX5))) {
      println("Create " + D_NX5)
      fs.mkdirs(new Path(D_NX5))
    }
    if (!fs.exists(new Path(D_NX5+"/in"))) {
      println("Create " + D_NX5+"/in")
      fs.mkdirs(new Path(D_NX5+"/in"))
    }
    if (!fs.exists(new Path(D_NX4+"/Done"))) {
      println("Create " + D_NX4+"/Done")
      fs.mkdirs(new Path(D_NX4+"/Done"))
    }

    val sparkenv = new spark_env
    val sqlContext = sparkenv.init()
    import sqlContext.implicits._

    val filetype = filein.replaceFirst("_.*", "")
    val fileenginename = filein.replaceFirst(filetype+"_", "").replaceFirst("_.*", "")
    val filedate=filein.replaceFirst(filetype+"_", "").replaceFirst(fileenginename+"_", "").replaceFirst("_.*", "")
    val fileday=filein.replaceFirst(filetype+"_", "").replaceFirst(fileenginename+"_", "").replaceFirst(filedate+"_", "").replaceFirst("\\.parquet$", "")

    println("pipeline4to5() : read filein : " + filein)
    val df = sqlContext.read.parquet(D_NX4 + "/in/" + filein)
    df.cache()

    println("pipeline4to5() : SiteResolution")
    val dfSite = SiteResolutionFromIP(sqlContext, df)

    println("pipeline4to5() : Sector resolution")
    val dfSiteSector = SiteAndSectorResolutionFromI_ID(sqlContext, dfSite)

    //Web request resolution
    println("pipeline4to5() : Web request resolution")
    val webrequest_filename = D_NX4 + "/in/" + "webrequest_" + fileenginename + "_" + filedate + "_" + fileday + ".parquet"
    val dfSiteSector4WebRequest = {
      if (fs.globStatus(new Path(webrequest_filename)).length == 0) {
        println("Error : corresponding web request file not found : " + webrequest_filename)
        null
      }else {
        //Read web request file
        val dfWebRequest = sqlContext.read.parquet(webrequest_filename)
        val dfres = WebRequestResolution(sqlContext, dfSiteSector, dfWebRequest)
        dfres
      }
    }
    if (dfSiteSector4WebRequest == null) {
      return false
    }

    //AIP resolution
    println("pipeline4to5() : AIP resolution")
    val dfSiteSector4WebRequestAIP = AIPResolution(sqlContext, dfSiteSector4WebRequest)

    println("pipeline4to5() : write result : " + D_NX5+"/in/"+filein)
    dfSiteSector4WebRequestAIP.write.format("parquet").mode("overwrite").save(D_NX5+"/in/"+filein)

    println("pipeline4to5() : move file :" + D_NX4 + "/in/" + filein + " -> " + D_NX4 + "/Done/" + filein)
    fs.rename(new Path(D_NX4 + "/in/" + filein), new Path(D_NX4 + "/Done/" + filein))
    fs.rename(new Path(webrequest_filename), new Path(D_NX4 + "/Done/webrequest_" + fileenginename + "_" + filedate + "_" + fileday + ".parquet"))

    return true
  }

  def SiteResolutionFromIP(sqlContext: org.apache.spark.sql.SQLContext, df: DataFrame): DataFrame = {
    import sqlContext.implicits._

    //Read MDM repository file
    val dfMDM = sqlContext.read.parquet(MDMRepository)
      .withColumn("mdm_ip_start_int", ip2Long($"mdm_ip_start"))
      .withColumn("mdm_ip_end_int", rangeToIP($"mdm_ip_start_int", $"mdm_ip_range"))
      .drop("mdm_ip_start").drop("mdm_ip_end").drop("mdm_ip_range")

    dfMDM.cache()

    //extract IP to resolve
    val dfIpInt = df
      .withColumn("dest_ip_int", dlutil.ip2Long($"dest_ip"))
      .withColumn("source_ip_int", dlutil.ip2Long($"source_ip"))

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
    // drop result columns from dfIpInt (Source_sector, source_I_ID_site) if already exist
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

  def SiteAndSectorResolutionFromI_ID(sqlContext: org.apache.spark.sql.SQLContext, df: DataFrame): DataFrame = {
    import sqlContext.implicits._
    val dfI_ID = sqlContext.read.parquet(I_IDRepository)
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
    val dfIID = df1.join(dfI_ID, df("I_ID_U") === dfI_ID("I_ID2"), "left_outer").drop("I_ID2")

    return dfIID

  }

  def AIPResolution(sqlContext: org.apache.spark.sql.SQLContext, df: DataFrame): DataFrame = {
    import sqlContext.implicits._

    //TO DO

    return df

  }

  def WebRequestResolution(sqlContext: org.apache.spark.sql.SQLContext, df: DataFrame, dfWebRequest: DataFrame): DataFrame = {
    import sqlContext.implicits._

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
        df("I_ID_D") <=> df4Web("wr_I_ID_D")
          && df("source_app_name") <=> df4Web("wr_source_app_name")
          && df("dest_ip") <=> df4Web("wr_dest_ip")
          && df("dest_port") <=> df4Web("wr_dest_port")
          && df("enddate") <=> df4Web("wr_enddate"),
        "left_outer")
      .drop("wr_I_ID_D")
      .drop("wr_source_app_name")
      .drop("wr_dest_ip")
      .drop("wr_dest_port")
      .drop("wr_enddate")

    return dfres

  }
}
