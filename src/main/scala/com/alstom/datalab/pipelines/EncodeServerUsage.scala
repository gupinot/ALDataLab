package com.alstom.datalab.pipelines

import com.alstom.datalab.{Meta, Pipeline, ControlFile}
import com.alstom.datalab.Util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Created by guillaumepinot on 25/01/2016.
  */
  class EncodeServerUsage(implicit sqlContext: SQLContext) extends Pipeline with Meta {

    import sqlContext.implicits._
    val metrics=Map(
      "iostat"->List("used_mb", "used_percent", "avail_mb", "diskq_count", "disktime_msec"),
      "sysstat"->List("cpu_percent", "memvirtualusage_percent", "swapusage_percent", "memphysusage_percent")
    )
    val fixname = sqlContext.udf.register("fixname",(name: String) => if (name == "processor utilization - overall cpu") "cpu_percent" else name)
    val gettable = sqlContext.udf.register("gettable", (table: String) => table.substring(0,table.indexOf(':')))
    val gettags = sqlContext.udf.register("gettags", (table: String) => table.substring(table.indexOf(':')+1))
    val extracttag = sqlContext.udf.register("get_tag",(tags:String,tagname:String) =>{
      val values = tags.split(",").toSeq.filter(_.startsWith(tagname)).flatMap(_.split("=").tail)
      if (values.size > 0) values(0) else ""
    })

    def execute(): Unit = {

      if (this.inputFiles.length == 1 && this.inputFiles.contains("nothing") ) {
        aggregate(context.dirout(), context.diragg())
      } else {

        this.inputFiles.foreach(filein => {

          val usagedata = sqlContext.read.json(filein).filter("table != 'app'")
          usagedata.cache().registerTempTable("usage")

          val frames = List("iostat","sysstat").map((tablename) => {
            val df = sqlContext.sql(s"""select date,server,fixname(name) as name,value,gettags(table) as tags from usage where table like '${tablename}%'""")
            val cols = metrics.getOrElse(tablename,df.select("name").distinct.collect.toList.map(_.getString(0)))
            val df_pivot = df.groupBy("date","server","name","tags").pivot("name",cols).avg("value")
            df_pivot.registerTempTable(tablename+"_pivot")
            df_pivot
          })

          sqlContext.sql("""
                   select date, server, mount, device, ceil(if(used_mb=0,avail_mb*used_percent/(1-used_percent),used_mb)) as used_mb, round(100*if (used_percent=0 or used_percent is null,used_mb/(used_mb+avail_mb),used_percent))/100 as used_percent, ceil(avail_mb) as avail_mb, ceil(if (total_mb=0,used_mb+avail_mb,total_mb)) as total_mb, diskq_count, disktime_msec
                   from (select cast(date/1000 as timestamp) as date,server,get_tag(tags,"mount") as mount, get_tag(tags,"device") as device, ceil(sum(used_mb)) as used_mb, sum(used_percent)/100 as used_percent, sum(avail_mb) as avail_mb, ceil(sum(avail_mb)/(1-sum(used_percent/100))) as total_mb, sum(diskq_count) as diskq_count, sum(disktime_msec) as disktime_msec from iostat_pivot
                   group by date, server, tags) u""").write.mode(SaveMode.Append).parquet(s"${context.dirout()}/iostat")

          sqlContext.sql("""
                  select cast(date/1000 as timestamp) as date,server,tags,sum(cpu_percent) as cpu_percent, sum(memvirtualusage_percent) as memvirtualusage_percent, sum(swapusage_percent) as swapusage_percent, sum(memphysusage_percent) as memphysusage_percent from sysstat_pivot
                  group by date, server, tags""").write.mode(SaveMode.Append).parquet(s"${context.dirout()}/sysstat")
        })
        try {
          aggregate(context.dirout(), context.diragg())
        }
        catch {
          case _: Throwable => "ko"
        }
      }
    }

    def aggregate(dirin: String, dirout: String) = {
      val dfStorage = context.repo().readMasterStorage()

      val dfAIP = context.repo().readAIP().withColumnRenamed("aip_server_ip", "server_ip")
                    .withColumn("aip_server_hostname", lower($"aip_server_hostname"))

      def deviceFormat = udf(
        (device: String, mount: String) => if(device == null || device == "") mount else device
      )

      def transcodetype = udf(
        (input: String) => input match {
          case null => "notFound"
          case _ => input
        }
      )

      val iostatorig = sqlContext.read.parquet(s"${dirin}/iostat")
        .withColumn("server", lower($"server"))
        .withColumn("device", lower(deviceFormat($"device", $"mount")))
        .filter("total_mb is not null")
        .withColumn("dt", to_date($"date"))
        .withColumn("month", date_format(to_date($"date"),"yyyy-MM"))

      val iostat = iostatorig
        .join(dfStorage, iostatorig("server") === dfStorage("server") && iostatorig("device") === dfStorage("mount"), "left_outer")
        .select((iostatorig.columns.map(iostatorig.col)
          ++ List(dfStorage("type"), dfStorage("charged_used_mb"), dfStorage("charged_total_mb"))):_*)
        .withColumn("type", transcodetype(dfStorage("type")))
        .join(dfAIP, iostatorig("server") === dfAIP("aip_server_hostname"), "left_outer")
        .withColumn("percent_avail", round($"avail_mb"/$"total_mb", 2))

      iostat
        .groupBy("server", "server_ip", "device", "dt", "month")
        .agg(
          approxCountDistinct($"date").as("count_date"),
          first($"type").as("type"),
          first($"aip_server_function").as("aip_server_function"),
          first($"aip_app_name").as("aip_app_name"),
          first($"aip_app_sector").as("aip_app_sector"),
          max($"total_mb").as("max_total_mb"),
          min($"avail_mb").as("q0_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.10)), 2).as("q10_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.25)), 2).as("q25_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.50)), 2).as("q50_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.75)), 2).as("q75_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.90)), 2).as("q90_avail_mb"),
          max($"avail_mb").as("q100_avail_mb"),
          min($"percent_avail").as("q0_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.10)), 2).as("q10_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.25)), 2).as("q25_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.50)), 2).as("q50_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.75)), 2).as("q75_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.90)), 2).as("q90_percent_avail"),
          max($"percent_avail").as("q100_percent_avail"),
          first($"charged_used_mb").as("charged_used_mb"),
          first($"charged_total_mb").as("charged_total_mb")
          )
        .write.mode(SaveMode.Overwrite).parquet(s"${dirout}/iostat_day")

      iostat
        .groupBy("server", "server_ip", "device", "month")
        .agg(
          approxCountDistinct($"date").as("count_date"),
          first($"type").as("type"),
          first($"aip_server_function").as("aip_server_function"),
          first($"aip_app_name").as("aip_app_name"),
          first($"aip_app_sector").as("aip_app_sector"),
          max($"total_mb").as("max_total_mb"),
          min($"avail_mb").as("q0_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.10)), 2).as("q10_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.25)), 2).as("q25_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.50)), 2).as("q50_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.75)), 2).as("q75_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.90)), 2).as("q90_avail_mb"),
          max($"avail_mb").as("q100_avail_mb"),
          min($"percent_avail").as("q0_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.10)), 2).as("q10_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.25)), 2).as("q25_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.50)), 2).as("q50_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.75)), 2).as("q75_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.90)), 2).as("q90_percent_avail"),
          max($"percent_avail").as("q100_percent_avail"),
          first($"charged_used_mb").as("charged_used_mb"),
          first($"charged_total_mb").as("charged_total_mb")
        )
        .write.mode(SaveMode.Overwrite).parquet(s"${dirout}/iostat_month")

      iostat
        .groupBy("server", "server_ip", "device")
        .agg(
          approxCountDistinct($"date").as("count_date"),
          first($"type").as("type"),
          first($"aip_server_function").as("aip_server_function"),
          first($"aip_app_name").as("aip_app_name"),
          first($"aip_app_sector").as("aip_app_sector"),
          max($"total_mb").as("max_total_mb"),
          min($"avail_mb").as("q0_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.10)), 2).as("q10_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.25)), 2).as("q25_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.50)), 2).as("q50_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.75)), 2).as("q75_avail_mb"),
          round(callUDF("percentile_approx", col("avail_mb"), lit(0.90)), 2).as("q90_avail_mb"),
          max($"avail_mb").as("q100_avail_mb"),
          min($"percent_avail").as("q0_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.10)), 2).as("q10_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.25)), 2).as("q25_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.50)), 2).as("q50_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.75)), 2).as("q75_percent_avail"),
          round(callUDF("percentile_approx", col("percent_avail"), lit(0.90)), 2).as("q90_percent_avail"),
          max($"percent_avail").as("q100_percent_avail"),
          first($"charged_used_mb").as("charged_used_mb"),
          first($"charged_total_mb").as("charged_total_mb")
        )
        .write.mode(SaveMode.Overwrite).parquet(s"${dirout}/iostat")


      //
      val sysstatorig = sqlContext.read.parquet(s"${dirin}/sysstat")
              .withColumn("dt", to_date($"date"))
              .withColumn("month", date_format(to_date($"date"),"yyyy-MM"))
              .withColumn("server", lower($"server"))

      val sysstat = sysstatorig
        .join(dfAIP, sysstatorig("server") === dfAIP("aip_server_hostname"), "left_outer")

      sysstat
        .filter("cpu_percent is not null")
        .groupBy("server", "server_ip", "dt", "month")
        .agg(
          approxCountDistinct($"date").as("count_date"),
          first($"aip_server_function").as("aip_server_function"),
          first($"aip_app_name").as("aip_app_name"),
          first($"aip_app_sector").as("aip_app_sector"),
          round(avg($"cpu_percent"),2).as("avg_cpu_percent"),
          round(min($"cpu_percent"), 2).as("q0_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.10)), 2).as("q10_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.25)), 2).as("q25_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.50)), 2).as("q50_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.75)), 2).as("q75_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.90)), 2).as("q90_cpu_percent"),
          round(max($"cpu_percent"),2).as("q100_cpu_percent")
        )
        .write.mode(SaveMode.Overwrite).parquet(s"${dirout}/sysstat_day")
      sysstat
        .filter("cpu_percent is not null")
        .groupBy("server", "server_ip", "month")
        .agg(
          approxCountDistinct($"date").as("count_date"),
          first($"aip_server_function").as("aip_server_function"),
          first($"aip_app_name").as("aip_app_name"),
          first($"aip_app_sector").as("aip_app_sector"),
          round(avg($"cpu_percent"),2).as("avg_cpu_percent"),
          round(min($"cpu_percent"), 2).as("q0_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.10)), 2).as("q10_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.25)), 2).as("q25_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.50)), 2).as("q50_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.75)), 2).as("q75_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.90)), 2).as("q90_cpu_percent"),
          round(max($"cpu_percent"),2).as("q100_cpu_percent")
        )
        .write.mode(SaveMode.Overwrite).parquet(s"${dirout}/sysstat_month")
      sysstat
        .filter("cpu_percent is not null")
        .groupBy("server", "server_ip")
        .agg(
          approxCountDistinct($"date").as("count_date"),
          first($"aip_server_function").as("aip_server_function"),
          first($"aip_app_name").as("aip_app_name"),
          first($"aip_app_sector").as("aip_app_sector"),
          round(avg($"cpu_percent"),2).as("avg_cpu_percent"),
          round(min($"cpu_percent"), 2).as("q0_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.10)), 2).as("q10_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.25)), 2).as("q25_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.50)), 2).as("q50_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.75)), 2).as("q75_cpu_percent"),
          round(callUDF("percentile_approx", col("cpu_percent"), lit(0.90)), 2).as("q90_cpu_percent"),
          round(max($"cpu_percent"),2).as("q100_cpu_percent")
        )
        .write.mode(SaveMode.Overwrite).parquet(s"${dirout}/sysstat")
    }
  }