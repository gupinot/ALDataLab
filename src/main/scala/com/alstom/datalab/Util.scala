package com.alstom.datalab

import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructType}

/**
  * Created by raphael on 01/12/2015.
  */
object Util {
  ///////////////////////////////////////////////////////////////////////////
  //////////////////////////////  UDF //////////////////////////////

  def basename(filepath: String):String = {
    val stripped = if (filepath.charAt(filepath.length-1)=='/') filepath.substring(0,filepath.length-1) else filepath
    stripped.substring(Math.max(0,stripped.lastIndexOf("/")+1),stripped.length)
  }

  def getFirst(pattern: scala.util.matching.Regex) = udf(
    (datetime: String) => pattern.findFirstIn(datetime) match {
      case Some(day) => day
      case None => ""
    }
  )

  def to_int = udf(
    (num: String) => try {
      num.toInt
    } catch {
      case e:Throwable => -1
    }
  )

  def collect_type(enginename: String) = enginename match {
    case "sabad15034.ad.sys" | "sacch15002.ad.sys" | "sumhg15005.dom1.ad.sys" => "server"
    case _ => "device"
  }

  def getCollectType = udf((enginename: String) => collect_type(enginename))

  val daypattern: scala.util.matching.Regex = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
  val timepattern: scala.util.matching.Regex = """(\d\d):(\d\d):(\d\d)""".r
  val iprangepattern: scala.util.matching.Regex = """(\d\d)""".r
  val ipinternalpattern: scala.util.matching.Regex = """10\.((([0-9])+)\.)+{2}([0-9]+)""".r

  def formatSite = udf (
    (site: String) => {
      if (site == null) "nf" else site
    }
  )

  def regexudf(pattern: scala.util.matching.Regex) = udf(
    (valeur: String) => pattern.findFirstIn(valeur) match {
      case Some(res) => true
      case None => false
    }
  )

  def aton = udf ((ip: String) => ip.split("\\.").map(_.toInt).foldLeft(0)((acc,b)=>(acc << 8) + b))

  def range = udf ((start: Int, stop: Int) => (start+1 until stop).toArray)

  //convert IP to integer
  def ip2Long = udf (
    (ip: String) => {
      val atoms: Array[Long] = ip.split("\\.").map(java.lang.Long.parseLong(_))
      val result: Long = (3 to 0 by -1).foldLeft(0L)(
        (result, position) => result | (atoms(3 - position) << position * 8))
      result & 0xFFFFFFFF
    }.toInt)

  //convert /XX to IP
  def rangeToIP = udf (
    (ipstartint: Int, range: Int) => {
      ipstartint + math.pow(2,32-range.toInt).toInt -1
    })

  //UDAF to concatenate multiple row in agg()
  class ConcatString(separator: String) extends UserDefinedAggregateFunction {

    def inputSchema: StructType =
      new StructType().add("site", StringType)
    def bufferSchema: StructType =
      new StructType().add("bigsite", StringType)
    // returns just a double: the sum
    def dataType: DataType = StringType
    // always gets the same result
    def deterministic: Boolean = true

    // each partial sum is initialized to zero
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, "")
    }

    // an individual sales value is incorporated by adding it if it exceeds 500.0
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val concat = buffer.getString(0)
      if (!input.isNullAt(0)) {
        val site = input.getString(0)
        if (concat !="" || site!="") {
          if (concat == "") {
            buffer.update(0, site)
          }else if (site == "") {
            buffer.update(0, concat)
          }else {
            buffer.update(0, concat+separator+site)
          }
        }
      }
    }

    // buffers are merged by adding the single values in them
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val concat = buffer1.getString(0)
      val site = buffer2.getString(0)
      if (concat !="" || site!="") {
        if (concat == "") {
          buffer1.update(0, site)
        }else if (site == "") {
          buffer1.update(0, concat)
        }else {
          buffer1.update(0, concat+separator+site)
        }
      }
    }

    // the aggregation buffer just has one value: so return it
    def evaluate(buffer: Row): Any = {
      buffer.getString(0)
    }
  }

  //UDAF to concatenate multiple row (unique) in agg()
  class ConcatUniqueString(separator: String) extends UserDefinedAggregateFunction {

    def inputSchema: StructType =
      new StructType().add("site", StringType)
    def bufferSchema: StructType =
      new StructType().add("bigsite", StringType)
    // returns just a double: the sum
    def dataType: DataType = StringType
    // always gets the same result
    def deterministic: Boolean = true

    // each partial sum is initialized to zero
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, "")
    }

    // an individual sales value is incorporated by adding it if it exceeds 500.0
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val concat = buffer.getString(0)
      if (!input.isNullAt(0)) {
        val site = input.getString(0)
        if (concat !="" || site!="") {
          if (concat == "") {
            buffer.update(0, site)
          }else if (site == "") {
            buffer.update(0, concat)
          }else {
            val res = concat+separator+site
            buffer.update(0, res.split(separator).distinct.mkString(separator))
          }
        }
      }
    }

    // buffers are merged by adding the single values in them
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val concat = buffer1.getString(0)
      val site = buffer2.getString(0)
      if (concat !="" || site!="") {
        if (concat == "") {
          buffer1.update(0, site)
        }else if (site == "") {
          buffer1.update(0, concat)
        }else {
          val res = concat+separator+site
          buffer1.update(0, res.split(separator).distinct.mkString(separator))
        }
      }
    }

    // the aggregation buffer just has one value: so return it
    def evaluate(buffer: Row): Any = {
      buffer.getString(0)
    }
  }

  def retValue(value: String) = udf((colval: String) => value)

  def countSeparator(splitcar: String) = udf(
    (chaine: String) => chaine.split(splitcar).length
  )

  def resolveSiteFromAIPIp(df: DataFrame, AIPPath: String, sqlContext: SQLContext) = {
    import sqlContext.implicits._

    val AIP = sqlContext.read.parquet(AIPPath)
      .select($"aip_server_ip" as "IP", $"aip_server_site" as "site")
      .filter($"site" === null or $"site" === "")
      .dropDuplicates(Array("IP"))

    df.join(AIP, df("IP") === AIP("IP"), "left_outer")
      .drop(AIP("IP"))

  }
}
