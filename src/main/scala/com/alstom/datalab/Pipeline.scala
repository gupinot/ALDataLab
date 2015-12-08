package com.alstom.datalab

import java.util.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


/**
  * Basic Pipeline trait that can be executed from the Main class
  */
trait Pipeline extends Serializable {
  var dirout:String = ""
  var direrr:String = ""
  var dirin: String = ""
  var dircontrol:String = ""
  var inputFiles:List[String] = List()
  var repo:Repo = null

  def context(repo: Repo):Pipeline = {this.repo =repo; this}
  def input(filein: String):Pipeline = input(List(filein))
  def error(direrr: String):Pipeline = {this.direrr = direrr; this }
  def input(filein: List[String]):Pipeline = { inputFiles = filein; this }
  def inputdir(dirIn: String):Pipeline = { dirin = dirIn; this }
  def output(dirout: String):Pipeline = { this.dirout = dirout; this }
  def control(dircontrol: String):Pipeline = { this.dircontrol = dircontrol; this}

  case class ControlFile(stage: String, jobid: String, filetype: String, collecttype: String, engine: String, filedt: String, status:String, day:String)

  def execute():Unit
}
