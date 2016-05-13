package com.alstom.datalab.pipelines

import com.alstom.datalab.{Repo, Pipeline}
import com.alstom.datalab.Util._
import org.apache.spark.sql.{SaveMode, SQLContext}
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

  def gb2mb = udf(
    (num: String) => try {
      Math.round(num.replace(',', '.').toDouble*1000)
    } catch {
      case e:Throwable => 0
    }
  )

  def transcodeTiers = udf(
    (tier: String) => tier match {
      case "Tier 1" => "SAN replicated DR"
      case "Tier 2" => "SAN"
      case "Tier 3" => "NAS"
      case "DAS" => "DAS"
      case "Local" => "Local"
      case "Exclude" => "Exclude"
      case _ => "Unknown"
    }
  )

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

      val res = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .option("parserLib", "UNIVOCITY")
        .option("charset", "windows-1252")
        .load(filein)

      val res2 = filetype match {

        case "MDM-ITC" => res
          .filter($"Status" === "Live" or $"Status" === "New").filter($"Active / Inactive" === "ON")
          .select(
            monotonicallyIncreasingId().as("mdm_id"),
            $"Location Code".as("mdm_loc_code"),
            $"Location Name".as("mdm_loc_name"),
            $"Country Code".as("mdm_loc_country"),
            regexp_replace($"IP Address", "[^0-9.]", "").as("mdm_ip_start"),
            regexp_replace($"Mask", "[^0-9]", "").cast("int").as("mdm_ip_range"),
            lit(filedate).as("filedate")
          )
          .filter($"mdm_ip_range" > 0)
          .withColumn("mdm_ip_start_int", aton($"mdm_ip_start"))
          .withColumn("mdm_ip_end_int", rangeToIP($"mdm_ip_start_int", $"mdm_ip_range"))

        case "AIP-Server" => res.select(
          $"Host name".as("aip_server_hostname"),
          $"Physical Server name".as("aip_server_phys_host"),
          $"Size".as("aip_server_size"),
          $"Function".as("aip_server_function"),
          $"Type".as("aip_server_type"),
          $"Sub-Function".as("aip_server_subfunction"),
          $"IP address".as("aip_server_ip"),
          $"For Hosting Virtual Servers Only".as("aip_server_vmw_host"),
          $"Status".as("aip_server_status"),
          $"Site code".as("aip_server_site"),
          $"Site Name".as("aip_server_site_name"),
          $"Leased or owned".as("aip_server_ownership"),
          $"CPU Name".as("aip_server_cpu_type"),
          $"CPUs number".as("aip_server_cpu_num"),
          $"Core number".as("aip_server_cpu_cores"),
          $"Owner".as("aip_server_owner"),
          $"Owner Org UID".as("aip_server_owner_org_id"),
          $"Vendor".as("aip_server_vendor"),
          $"Model".as("aip_server_model"),
          //$"Installation date".as("aip_server_dt_install"),
          $"Operational role".as("aip_server_role"),
          $"Source".as("aip_server_source"),
          $"Administrated by".as("aip_server_adminby"),
          $"OS Name".as("aip_server_os_name"),
          lit(filedate).as("filedate")
        )

        case "AIP-Application" => res.select(
          $"Application Name".as("aip_app_name"),
          $"Shared Unique ID".as("aip_app_shared_unique_id"),
          $"Type".as("aip_app_type"),
          $"Validation".as("aip_app_validation"),
          $"Current State".as("aip_app_state"),
          $"Sensitive Application".as("aip_app_sensitive"),
          $"Alstom Criticality".as("aip_app_criticality"),
          $"Sector".as("aip_app_sector"),
          $"IT Owner".as("aip_app_it_owner"),
          $"IS Owner".as("aip_app_is_owner"),
          lit(filedate).as("filedate")
        )

        case "AIP-AppGFtoApp" => res.select(
          $"Name".as("aip_appgf_name"),
          $"Shared Unique ID".as("aip_appgf_shared_unique_id"),
          $"Type".as("aip_appgf_type"),
          $"GridFusion Application Name".as("aip_appgf_grid_app_name"),
          $"Billing Code".as("aip_appgf_billing_code"),
          lit(filedate).as("filedate")
        )

        case "AIP-SoftInstance" => res.select(
          $"Application name".as("aip_appinstance_name"),
          $"Shared Unique ID".as("aip_appinstance_shared_unique_id"),
          $"Type".as("aip_appinstance_type"),
          $"Host name".as("aip_appinstance_hostname"),
          $"IP address".as("aip_appinstance_ip"),
          $"Application Sector".as("aip_appinstance_sector"),
          $"Application current state".as("aip_appinstance_state"),
          lit(filedate).as("filedate")
        )

        case "AIP-OrgDeploy" => res.select(
          $"Application Name".as("aip_orgdeploy_name"),
          $"Shared Unique ID".as("aip_orgdeploy_shared_unique_id"),
          $"Org unit UID".as("aip_orgdeploy_unit_uid"),
          $"Org unit Name".as("aip_orgdeploy_unit_name"),
          $"Org unit Teranga code".as("aip_orgdeploy_unit_terranga_code"),
          $"Org unit Type".as("aip_orgdeploy_unit_type"),
          $"Expected number of users".as("aip_orgdeploy_users_nb"),
          lit(filedate).as("filedate")
        )

        case "AIP-Software" => res.select(
          $"Application".as("aip_software_name"),
          $"Unique ID".as("aip_software_shared_unique_id"),
          $"GAPM ID".as("aip_software_gapm_id"),
          $"ADC Support".as("aip_software_adc_support"),
          $"Application Based on".as("aip_software_app_based_on"),
          $"Main Product ?".as("aip_software_main_product"),
          $"Editor".as("aip_software_editor"),
          $"Product Name".as("aip_software_product_name"),
          $"Product Type".as("aip_software_product_type"),
          $"Product Version".as("aip_software_product_version"),
          $"Product Edition".as("aip_software_product_edition"),
          lit(filedate).as("filedate")
        )

        case "I-ID" => res.select(
          $"I_ID",
          $"SiteCode",
          $"SiteName",
          $"CountryCode",
          $"Sector",
          $"TerangaCode",
          lit(filedate).as("filedate")
        )
        case "Storage-Master-Report" => res
          .withColumn("server", lower($"sServerName"))
          .withColumn("mount", lower($"sPartition"))
          .withColumn("type", transcodeTiers($"StorageTier"))
          .filter($"ServerBillableForStorage" === "Yes")
          .withColumn("charged_total_mb", gb2mb($"TotalMeassureGb"))
          .withColumn("charged_used_mb", gb2mb($"UsedMeassureGb"))
          .select(
            $"server".as("server"),
            $"mount".as("mount"),
            $"type".as("type"),
            $"charged_used_mb".as("charged_used_mb"),
            $"charged_total_mb".as("charged_total_mb"),
            lit(filedate).as("filedate"))
        case _ => println(s"RepoProcessInFile() : filetype ($filetype) not match")
          sys.exit(1)
      }

      res2.write.mode(SaveMode.Append).partitionBy("filedate").parquet(respath)
      println("RepoProcessInFile() : load.write done")
    })
  }
}
