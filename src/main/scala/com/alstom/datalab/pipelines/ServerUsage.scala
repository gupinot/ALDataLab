package com.alstom.datalab.pipelines

import com.alstom.datalab.{Meta, Pipeline, ControlFile}
import com.alstom.datalab.Util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Created by guillaumepinot on 25/01/2016.
  */
  class ServerUsage(implicit sqlContext: SQLContext) extends Pipeline with Meta {

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
      }
    }