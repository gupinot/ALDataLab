package com.alstom.datalab.pipelines

import com.alstom.datalab.Util._
import com.alstom.datalab.{MetaSockets, Pipeline}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType, DateType}

/**
  * Created by guillaumepinot on 16/02/2016.
  */

object EncodeServerSockets {
  val STAGE_NAME = "encodesockets"
}


class EncodeServerSockets (implicit sqlContext: SQLContext) extends Pipeline with MetaSockets {
  import sqlContext.implicits._

  def parseFiles(inputFiles: List[String]) = inputFiles.map(filein=> {
    val filename = basename(filein)
    val filetype = filename.substring(0,filename.indexOf('_'))

    InputRecord(filetype, filein)
  })


  val lsofSchema = StructType(Seq(
    StructField("server_ip", StringType, true),
    StructField("dt", StringType, true),
    StructField("command", StringType, true),
    StructField("pid", StringType, true),
    StructField("user", StringType, true),
    StructField("node", StringType, true),
    StructField("name", StringType, true),
    StructField("status", StringType, true)
  ))
  val psSchema = StructType(Seq(
    StructField("server_ip", StringType, true),
    StructField("dt", StringType, true),
    StructField("user", StringType, true),
    StructField("pid", StringType, true),
    StructField("ppid", StringType, true),
    StructField("time", StringType, true),
    StructField("command", StringType, true)
  ))

  override def execute(): Unit = {

    val metasockets =  aggregateMetaSockets(loadMetaSockets(context.meta()), EncodeServerSockets.STAGE_NAME)

    val inputs = parseFiles(this.inputFiles).groupBy(_.filetype)

    inputs.foreach(input=>{
      val filetype = input._1
      val records = input._2

      val resultByType = records.map((record)=> {

        val selectedDf = filetype match {
          case "lsof" =>
            sqlContext.read.format("com.databricks.spark.csv")
              .option("header", "false")
              .option("delimiter", ";")
              .option("mode", "DROPMALFORMED")
              .schema(lsofSchema)
              .load(record.filename)
              .withColumn("dt", regexp_replace($"dt", "(....)(..)(..)-(..)(..)", "$1-$2-$3T$4:$5"))
          case "ps" =>
            sqlContext.read.format("com.databricks.spark.csv")
              .option("header", "false")
              .option("delimiter", ";")
              .option("mode", "DROPMALFORMED")
              .schema(psSchema)
              .load(record.filename)
              .withColumn("dt", regexp_replace($"dt", "(....)(..)(..)-(..)(..)", "$1-$2-$3T$4:$5"))
        }
        selectedDf
      }).reduce(_.unionAll(_)).as("in")


      val newInput = resultByType.join(broadcast(metasockets).as("meta"),
        ($"in.dt" === $"meta.dt") and ($"in.server_ip" === $"meta.server_ip")
          and ($"meta.filetype" === lit(filetype)),
        "left_outer")
        .select("meta.dt",resultByType.columns.map("in."+_):_*)
        .filter("meta.dt is null")
        .drop($"meta.dt")
        .withColumn("month", date_format(to_date($"in.dt"),"yyyy-MM"))

      newInput.write.mode(SaveMode.Append)
        .partitionBy("month")
        .parquet(s"${context.dirout()}/$filetype")

      val updatedMeta = newInput.select(
        lit(filetype) as "filetype", lit(EncodeServerSockets.STAGE_NAME) as "stage",
        $"server_ip",$"dt").distinct().repartition(1)

      updatedMeta.write.mode(SaveMode.Append).parquet(context.meta())

    })
  }
}
