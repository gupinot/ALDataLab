package com.alstom.datalab.pipelines

import com.alstom.datalab.{Repo, Pipeline}
import com.alstom.datalab.Util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

/**
  * Created by raphael on 02/12/2015.
  */
class RepoProcessInFile(sqlContext: SQLContext) extends Pipeline {
  import sqlContext.implicits._

  val aipserverSchema = StructType(Seq(
    StructField("host_name", StringType, true),
    StructField("function", StringType, true),
    StructField("type", StringType, true),
    StructField("subfunction", StringType, true),
    StructField("skyscope", StringType, true),
    StructField("vmw_host", StringType, true),
    StructField("windfarm", StringType, true),
    StructField("ip", StringType, true),
    StructField("ipvalidation", StringType, true),
    StructField("size", StringType, true),
    StructField("SID", StringType, true),
    StructField("decomdate", StringType, true),
    StructField("status", StringType, true),
    StructField("serial", StringType, true),
    StructField("site_name", StringType, true),
    StructField("site_code", StringType, true),
    StructField("cpu_type", StringType, true),
    StructField("cpu_num", IntegerType, true),
    StructField("cpu_cores", IntegerType, true),
    StructField("memory_mb", IntegerType, true),
    StructField("owner", StringType, true),
    StructField("owner_org_id", StringType, true),
    StructField("owner_org_terranga_code", StringType, true),
    StructField("owner_org_terranga_name", StringType, true),
    StructField("admin_by", StringType, true),
    StructField("adminby_org_id", StringType, true),
    StructField("adminby_org_terranga", StringType, true),
    StructField("adminby_org_name", StringType, true),
    StructField("dt_purchase", StringType, true),
    StructField("dt_install", StringType, true),
    StructField("vendor", StringType, true),
    StructField("model", StringType, true),
    StructField("ola", StringType, true),
    StructField("csc_costs", StringType, true),
    StructField("dco_costs", StringType, true),
    StructField("role", StringType, true),
    StructField("linked_app", IntegerType, true),
    StructField("platform", StringType, true),
    StructField("ownership", StringType, true),
    StructField("phys_host", StringType, true),
    StructField("cat_india", StringType, true),
    StructField("os_name", StringType, true),
    StructField("os_edition", StringType, true),
    StructField("os_version", StringType, true),
    StructField("os_service_pack", StringType, true),
    StructField("os_arch", StringType, true),
    StructField("source", StringType, true)))


  override def execute(): Unit = {
    this.inputFiles.foreach(f = (filein) => {
      val filename = basename(filein)
      val Array(filetype, file_date) = filename.replaceAll("\\.[^_]+$", "").split("_")
      val (year, month_day) = file_date.splitAt(4)
      val (month, day) = month_day.splitAt(2)
      val filedate = s"$year-$month-$day"

      println("RepoProcessInFile() : filename=" + filein + ", filetype=" + filetype + ", filedate=" + filedate)
      val respath = s"${context.dirout()}/$filetype"
      println("RepoProcessInFile() : respath = " + respath)

      val res = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";")
        .option("inferSchema", "true").option("mode", "DROPMALFORMED").option("parserLib", "UNIVOCITY")
        .load(filein)

      val res2 = filetype match {

        case "MDM-ITC" => res
          .filter($"Status" === "Live" or $"Status" === "New").filter($"Active / Inactive" === "ON")
          .select(
            monotonicallyIncreasingId().as("mdm_id"),
            $"Location Code".as("mdm_loc_code"),
            regexp_replace($"IP Address", "[^0-9.]", "").as("mdm_ip_start"),
            regexp_replace($"Mask", "[^0-9]", "").cast("int").as("mdm_ip_range"),
            lit(filedate).as("filedate")
          )
          .filter($"mdm_ip_range" > 0)
          .withColumn("mdm_ip_start_int", aton($"mdm_ip_start"))
          .withColumn("mdm_ip_end_int", rangeToIP($"mdm_ip_start_int", $"mdm_ip_range"))

        case "AIP-Server" => res.select(
          $"Host name".as("aip_server_hostname"),
          $"Function".as("aip_server_function"),
          $"Type".as("aip_server_type"),
          $"Sub-Function".as("aip_server_subfunction"),
          $"IP address".as("aip_server_ip"),
          $"Status".as("aip_server_status"),
          $"Administrated by".as("aip_server_adminby"),
          $"OS Name".as("aip_server_os_name"),
          lit(filedate).as("filedate")
        )

        case "AIP-Application" => res.select(
          $"Application Name".as("aip_app_name"),
          $"Shared Unique ID".as("aip_app_shared_unique_id"),
          $"Type".as("aip_app_type"),
          $"Current State".as("aip_app_state"),
          $"Sensitive Application".as("aip_app_sensitive"),
          $"Criticality".as("aip_app_criticality"),
          $"Sector".as("aip_app_sector"),
          lit(filedate).as("filedate")
        )

        case "AIP-SoftInstance" => res.select(
          $"Application name".as("aip_appinstance_name"),
          $"Shared Unique ID".as("aip_appinstance_shared_unique_id"),
          $"Type".as("aip_appinstance_type"),
          $"Host name".as("aip_appinstance_hostname"),
          $"IP address".as("aip_appinstance_ip"),
          lit(filedate).as("filedate")
        )
        case "I-ID" => res.select(
          $"I_ID",
          $"SiteCode",
          $"Sector",
          lit(filedate).as("filedate")
        )
        case _ => println(s"RepoProcessInFile() : filetype ($filetype) not match")
          sys.exit(1)
      }

      res2.write.mode("append").partitionBy("filedate").parquet(respath)
      println("RepoProcessInFile() : load.write done")
    })
  }
}
