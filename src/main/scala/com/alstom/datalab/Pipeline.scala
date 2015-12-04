package com.alstom.datalab

import org.apache.spark.sql.SQLContext

/**
  * Basic Pipeline trait that can be executed from the Main class
  */
trait Pipeline extends Serializable {
  var dirout:String = ""
  var dircontrol:String = ""
  var inputFiles:List[String] = List()
  var repo:Repo = null

  def context(repo: Repo):Pipeline = {this.repo =repo; this}
  def input(filein: String):Pipeline = input(List(filein))
  def input(filein: List[String]):Pipeline = { inputFiles = filein; this }
  def output(dirout: String):Pipeline = { this.dirout = dirout; this }
  def control(dircontrol: String):Pipeline = { this.dircontrol = dircontrol; this}

  def execute():Unit
}
