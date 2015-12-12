package com.alstom.datalab

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


/**
  * Basic Pipeline trait that can be executed from the Main class
  */

case class ControlFile(stage: String, jobid: String, filetype: String, collecttype: String, engine: String, filedt: String, status:String, day:String) extends Serializable {
  override def toString = s"$stage;$jobid;$filetype;$collecttype;$engine;$filedt;$status;$day"
}

trait Pipeline extends Serializable {
  var inputFiles:List[String] = List()
  var context:Context = new Context(Map())

  def context(context: Context):Pipeline = {this.context =context; this}
  def input(files: List[String]):Pipeline = {this.inputFiles =files; this}

  def execute():Unit
}
