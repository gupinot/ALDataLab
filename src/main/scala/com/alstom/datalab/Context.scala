package com.alstom.datalab

import org.apache.spark.sql.SQLContext

import scala.collection.mutable.HashMap

/**
  * Created by raphael on 11/12/2015.
  */
object Context {
  val REPO="repo"
  val DEFAULT_REPO="hdfs:///data/repo"
  val DIRERR="direrr"
  val DEFAULT_DIRERR="hdfs:///data/err"
  val DIROUT="dirout"
  val DEFAULT_DIROUT="hdfs:///data/out/encoded"
  val DIRIN="dirin"
  val DEFAULT_DIRIN="hdfs:///data/out/encoded"
  val DIRAGG="diragg"
  val DEFAULT_DIRAGG="hdfs:///data/out/aggregated"
  val CONTROL="control"
  val DEFAULT_CONTROL="hdfs:///data/control"
  val META="meta"
  val DEFAULT_META="hdfs:///data/meta"
}

class Context(map: Map[String,String]) extends HashMap[String,String] {
  var repo:Repo=null

  map.foreach(e=>this.put(e._1,e._2))

  def repodir() = this.getOrElse(Context.REPO,Context.DEFAULT_REPO)
  def direrr() = this.getOrElse(Context.DIRERR,Context.DEFAULT_DIRERR)
  def dirout() = this.getOrElse(Context.DIROUT,Context.DEFAULT_DIROUT)
  def dirin() = this.getOrElse(Context.DIRIN,Context.DEFAULT_DIRIN)
  def diragg() = this.getOrElse(Context.DIRAGG,Context.DEFAULT_DIRAGG)
  def control() = this.getOrElse(Context.CONTROL,Context.DEFAULT_CONTROL)
  def meta() = this.getOrElse(Context.META,Context.DEFAULT_META)

  def repo(repo: Repo=null):Repo = { if (repo != null) this.repo=repo; this.repo }

  override def toString() = {
    this.map(e=>e._1+": "+e._2).mkString("\n")
  }
}
