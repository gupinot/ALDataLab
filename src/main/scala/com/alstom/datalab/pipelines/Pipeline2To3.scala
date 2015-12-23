package com.alstom.datalab.pipelines

import com.alstom.datalab.{Meta, Pipeline}
import com.alstom.datalab.Util._
import org.apache.spark.sql.types.{StructField, StringType, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by guillaumepinot on 05/11/2015.
  */
object Pipeline2To3 {
  val STAGE_NAME = "pipe2to3"
}

case class FileDescriptor(filetype:String, enginename:String, filedt:String, enginetype:String)
case class InputRecord(filetype: String,filename: String)

class Pipeline2To3(implicit sqlContext: SQLContext) extends Pipeline with Meta {
  import sqlContext.implicits._

  def parseFiles(inputFiles: List[String]) = inputFiles.map(filein=> {
    val filename = basename(filein)
    val filetype = filename.substring(0,filename.indexOf('_'))

    InputRecord(filetype, filein)
  })

  override def execute(): Unit = {
    val jobid:Long = System.currentTimeMillis/1000

    val metaDf = aggregateMeta(loadMeta(context.meta()), Pipeline2To3.STAGE_NAME)
    val inputs = parseFiles(this.inputFiles).groupBy(_.filetype)

    val resultMeta = inputs.map(input=>{
      val filetype = input._1
      val records = input._2

      val baseDf = records.map((record)=>{
        val df = sqlContext.read.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("mode", "DROPMALFORMED")
          //.option("parserLib", "UNIVOCITY")
          .load(record.filename)

        val selectedDf = filetype match {
          case "connection" => df.select(
            $"I_ID_D",
            $"I_ID_U",
            $"NX_con_start_time" as "con_start",
            $"NX_con_end_time" as "con_end",
            to_int($"NX_con_duration") as "con_duration",
            $"NX_con_cardinality" cast "int" as "con_number",
            $"NX_con_destination_ip" as "dest_ip",
            $"NX_con_out_traffic" cast "long" as "con_traffic_out",
            $"NX_con_in_traffic" cast "long" as "con_traffic_in",
            $"NX_con_type" as "con_protocol",
            $"NX_con_status" as "con_status",
            $"NX_con_port" cast "int" as "dest_port",
            $"NX_bin_app_category" as "source_app_category",
            $"NX_bin_app_company" as "source_app_company",
            $"NX_bin_app_name" as "source_app_name",
            $"NX_bin_exec_name" as "source_app_exec",
            $"NX_bin_paths" as "source_app_paths",
            $"NX_bin_version" as "source_app_version",
            $"NX_device_last_ip" as "source_ip",
            $"engine",
            $"filedt",
            getCollectType($"engine") as "collecttype",
            to_date($"NX_con_end_time") as "dt",
            date_format(to_date($"NX_con_end_time"),"yyyy-MM") as "month"
          )
          case "webrequest" => df.select(
            $"I_ID_D",
            $"wr_start_time" as "start",
            $"wr_end_time" as "end",
            $"wr_url" as "url",
            $"wr_destination_port" cast "int" as "dest_port",
            $"wr_destination_ip" as "dest_ip",
            $"wr_application_name" as "source_app_name",
            $"engine",
            $"filedt",
            getCollectType($"engine") as "collecttype",
            to_date($"wr_end_time") as "dt",
            date_format(to_date($"wr_end_time"),"yyyy-MM") as "month"
          )
          case _ => df
        }
        selectedDf
      }).reduce(_.unionAll(_))

      val endColumn = if (filetype == "connection") $"con_end" else $"end"

      val inputDf = (context.get("partition") match {
        case Some(partNum) => baseDf.coalesce(partNum.toInt)
        case None => baseDf
      }).as("in")

      val newInput = inputDf.join(broadcast(metaDf).as("meta"),
        ($"in.dt" === $"meta.dt") and ($"in.engine" === $"meta.engine")
          and ($"in.collecttype" === $"meta.collecttype") and ($"meta.filetype" === lit(filetype)),
        "left_outer")
        .select("min_filedt",inputDf.columns.map("in."+_):_*)
        .filter("min_filedt > filedt or min_filedt is null")
        .drop("min_filedt")

      val joinedCachedDf = context.get("no-cache") match {
        case Some(ftype) => if ((ftype == "all") || (filetype == filetype)) newInput else newInput.cache()
        case None => newInput.cache()
      }

      val completeDf = joinedCachedDf.groupBy($"collecttype",$"engine",$"dt",$"filedt")
        .agg(min(hour(endColumn)).as("min_hour"),max(hour(endColumn)).as("max_hour"))
        .filter($"min_hour" <= 3 and $"max_hour" >= 23)

      val resDf = if (filetype == "connection") {
        joinedCachedDf.as("all").join(broadcast(completeDf).as("complete"),
          ($"all.dt" === $"complete.dt") and ($"all.engine" === $"complete.engine")
            and ($"all.collecttype" === $"complete.collecttype") and ($"all.filedt" === $"complete.filedt"),"inner")
          .select(joinedCachedDf.columns.map(joinedCachedDf.col):_*)
      } else {
        joinedCachedDf
      }

      val cachedResDf = context.get("no-cache") match {
        case Some(ftype) => if ((ftype == "all") || (filetype == filetype)) resDf else resDf.cache()
        case None => resDf.cache()
      }

      cachedResDf.write.mode(SaveMode.Append)
        .partitionBy("month")
        .parquet(s"${context.dirout()}/$filetype")

      val updatedMeta = cachedResDf.select(
        lit(filetype) as "filetype", lit(Pipeline2To3.STAGE_NAME) as "stage",
        $"collecttype", $"engine",$"dt",$"filedt").distinct().repartition(1)

      updatedMeta.write.mode(SaveMode.Append).parquet(context.meta())
      updatedMeta
    })
  }
}
