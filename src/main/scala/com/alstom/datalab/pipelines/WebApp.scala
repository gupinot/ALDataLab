/**
  * Created by guillaumepinot on 28/11/2015.
  */
package com.alstom.datalab.pipelines

import java.awt.GraphicsDevice
import java.io.Serializable
import java.sql.Date

import com.alstom.datalab.Util.{ConcatUniqueString, ConcatString, countSeparator}
import com.alstom.datalab._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.{Row, SaveMode, DataFrame, SQLContext}


class WebApp(implicit sqlContext: SQLContext) extends Pipeline with Meta {

  import sqlContext.implicits._

  private val myIO = new com.alstom.datalab.IO

  def splitudf(splitcar: String) = udf(
    (chaine: String) => chaine.split(splitcar)
  )

  def aggregatedf(df: DataFrame, collecttype: String = "device", date_range: String, level: String, resolution: String) = {
    val myConcat = new ConcatUniqueString(",")
    val res = {
      if (collecttype == "device") {
        {
          if (level == "detail") {
            if (resolution == "IDM") {
              df.groupBy("source_sector", "source_I_ID_site", "source_teranga", "source_app_name", "source_app_category", "source_app_exec", "url",
                "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type",
                "source_aip_app_sector", "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
                "dest_ip", "dest_port", "con_protocol", "dest_site", "con_status",
                "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type",
                "dest_aip_app_sector", "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type")
            } else {
              df.groupBy("source_sector", "source_site", "source_teranga", "source_app_name", "source_app_category", "source_app_exec", "url",
                "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type",
                "source_aip_app_sector", "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
                "dest_ip", "dest_port", "con_protocol", "dest_site", "con_status",
                "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type",
                "dest_aip_app_sector", "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type")
            }
          } else {
            if (resolution == "IDM") {
              df.groupBy("source_sector", "source_I_ID_site", "source_teranga", "source_app_name", "source_app_category", "source_app_exec", "url",
                "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type",
                "source_aip_app_sector", "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
                "con_status", "dest_site",
                "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type",
                "dest_aip_app_sector", "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type")
            } else {
              df.groupBy("source_sector", "source_site", "source_teranga", "source_app_name", "source_app_category", "source_app_exec", "url",
                "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type",
                "source_aip_app_sector", "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
                "con_status", "dest_site",
                "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type",
                "dest_aip_app_sector", "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type")
            }
          }
        }.agg(sum($"con_number").as("con_number"),
          sum($"con_traffic_in" + $"con_traffic_out").as("con_traffic"),
          sum(($"con_traffic_in" + $"con_traffic_out")*($"con_number")).cast("bigint").as("con_times_traffic"),
          myConcat($"I_ID_U").as("I_ID_U"))
          .withColumn("distinct_I_ID_U", countSeparator(",")($"I_ID_U"))
      }else {
        {
          if (level == "detail") {
            df.groupBy("source_sector", "source_ip",
              "source_site", "source_app_name", "source_app_category", "source_app_exec", "url",
              "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type",
              "source_aip_app_sector", "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
              "dest_ip", "dest_port", "con_protocol", "dest_site", "con_status",
              "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type",
              "dest_aip_app_sector", "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type")
          } else {
            df.groupBy("source_sector", "source_ip",
              "source_site", "source_app_name", "source_app_category", "source_app_exec", "url",
              "source_aip_app_name", "source_aip_server_function", "source_aip_server_subfunction", "source_aip_app_criticality", "source_aip_app_type",
              "source_aip_app_sector", "source_aip_app_shared_unique_id", "source_aip_server_adminby", "source_aip_app_state", "source_aip_appinstance_type",
              "con_status", "dest_site",
              "dest_aip_app_name", "dest_aip_server_function", "dest_aip_server_subfunction", "dest_aip_app_criticality", "dest_aip_app_type",
              "dest_aip_app_sector", "dest_aip_app_shared_unique_id", "dest_aip_server_adminby", "dest_aip_app_state", "dest_aip_appinstance_type")
          }
        }
      }.agg(sum($"con_number").as("con_number"),
        sum($"con_traffic_in" + $"con_traffic_out").as("con_traffic"),
        sum(($"con_traffic_in" + $"con_traffic_out")*($"con_number")).cast("bigint").as("con_times_traffic"))
    }
    res
  }

  def generate(df: DataFrame, collecttype: String = "device", date_range: String, level: String, resolution: String) = {

    val filename1 = collecttype match {
      case "device" => if (level == "detail") s"${resolution}AppliSiteServer" else s"${resolution}AppliSite"
      case "server" => if (level == "detail") "AppliDetailServer2Server" else "AppliServer2Server"
    }
    val filename2 = date_range match {
      case "Full" => ""
      case _ => date_range
    }
    val fileout = s"Stat${filename1}${filename2}.csv.gz"

    myIO.writeCsvToS3(aggregatedf(df, collecttype, date_range, level, resolution), dstfile =  s"${fileout}", s3root = s"${context.dirout()}", skipVerify = true)
  }

  def execute(): Unit = {


    val jobidcur:Long = System.currentTimeMillis/1000

    //read meta to compute
    val meta45 = aggregateMeta(loadMeta(context.meta()), Pipeline4To5.STAGE_NAME).as("meta45")

    //keep only days with sufficient engines collected : 20 for device, 3 for server
    val dt_device = meta45
      .filter($"collecttype" === "device")
      .groupBy("dt")
      .agg(countDistinct($"engine").as("engine_count"))
      .filter("engine_count >= 20")
      .select($"dt")
      .distinct()
    val dt_server = meta45
      .filter($"collecttype" === "server")
      .groupBy("dt")
      .agg(countDistinct($"engine").as("engine_count"))
      .filter("engine_count >= 3")
      .select($"dt")
      .distinct()


    case class Date_Range(firstDate: String, lastDate: String)

    for( collecttype <- List("device", "server") ){
      val datelist = collecttype match {
        case "device" => dt_device
        case "server" => dt_server
      }
      val daylast = datelist.agg(max($"dt").as("dt")).collect().map(_.getDate(0).toString())
      for ( date_range <- List("6Weeks", "LastWeek", "DayOne", "Full") ) {
        val daymin = date_range match {
          case "DayOne" => Array("2015-11-02")
          case "6Weeks" => datelist.agg(max($"dt").as("dt")).withColumn("dt", date_sub($"dt", 42)).collect().map(_.getDate(0).toString())
          case "LastWeek" => datelist.agg(max($"dt").as("dt")).withColumn("dt", date_sub($"dt", 7)).collect().map(_.getDate(0).toString())
          case "Full" => datelist.agg(min($"dt").as("dt")).collect().map(_.getDate(0).toString())
        }
        val filedaterangeprefix = if (collecttype == "server") "DateRangeServer" else "DateRange"
        val filedaterange = if (date_range == "Full") s"${filedaterangeprefix}.csv.gz" else s"${filedaterangeprefix}${date_range}.csv.gz"

        val daterangeSchema =  StructType(Seq(
          StructField("firstDate", StringType, true),
          StructField("lastDate", StringType, true)))

        val rows = Seq(Row(daymin(0), daylast(0)))
        val tmp = sqlContext.sparkContext.parallelize(rows)
        val daterange = sqlContext.createDataFrame(tmp, daterangeSchema)

        myIO.writeCsvToS3(daterange, dstfile = s"${filedaterange}", s3root = s"${context.dirout()}", skipVerify = true)

        val aggregated = sqlContext.read.option("mergeSchema", "false").parquet(s"${context.dirin()}/${collecttype}")
        val aggregated_dtok =  aggregated
          .join(datelist.filter($"dt" >= daymin(0)), aggregated("dt") === datelist("dt"), "inner")
          .select(aggregated.columns.map(aggregated.col):_*)
          .withColumn("con_number", $"con_number".cast("bigint"))
          .withColumn("con_traffic_in", $"con_traffic_in".cast("bigint"))
          .withColumn("con_traffic_out", $"con_traffic_out".cast("bigint"))
          .na.fill("NA").na.fill(0)
        aggregated_dtok.cache()

        for (resolution <- List("IDM", "Device"); level <- List("detail", "nodetail")) {
          generate(aggregated_dtok, collecttype, date_range, level, resolution)
        }
      }
    }

    //Eval and write SectorCode file
    myIO.writeCsvToS3(context.repo().readI_ID().select("Sector").filter("Sector is not null").distinct(), dstfile = "SectorCode.csv.gz", s3root = s"${context.dirout()}", skipVerify = true)

    //eval and write SiteCode file
    val SiteCode = context.repo().readI_ID()
      .select(
        $"SiteCode",
        $"SiteName",
        $"CountryCode"
      ).distinct()
      .unionAll(
        context.repo().readMDM()
          .select(
            $"mdm_loc_code".as("SiteCode"),
            $"mdm_loc_name".as("SiteName"),
            $"mdm_loc_country".as("CountryCode")
          )
          .distinct())
      .groupBy("SiteCode")
      .agg(first($"SiteName").as("SiteName"), first($"CountryCode").as("CountryCode"))

    myIO.writeCsvToS3(SiteCode,
        dstfile = "SiteCode.csv.gz", s3root = s"${context.dirout()}", skipVerify = true)
  }
}

