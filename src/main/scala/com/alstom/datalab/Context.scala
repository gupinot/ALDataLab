package com.alstom.datalab

import org.apache.spark.sql.SQLContext

import scala.collection.mutable.HashMap

/**
  * Created by raphael on 11/12/2015.
  */
object Context {
  val REPO="context.repo"
  val DEFAULT_REPO="s3://alstomlezoomerus/DATA/Repository"
  val DIRERR="context.direrr"
  val DEFAULT_DIRERR="s3://alstomlezoomerus/DATA/2-err"
  val DIROUT="context.dirout"
  val DEFAULT_DIROUT="s3://alstomlezoomerus/DATA/2-out"
  val DIRIN="context.dirin"
  val DEFAULT_DIRIN="s3://alstomlezoomerus/DATA/2-in"
  val CONTROL="context.control"
  val DEFAULT_CONTROL="s3://alstomlezoomerus/DATA/2-control"
  val META="context.meta"
  val DEFAULT_META="s3://alstomlezoomerus/DATA/2-meta"
}

class Context(map: Map[String,String]) extends HashMap[String,String] {
  var repo:Repo=null

  map.foreach(e=>this.put(e._1,e._2))

  def repodir() = this.getOrElse(Context.REPO,Context.DEFAULT_REPO)
  def direrr() = this.getOrElse(Context.DIRERR,Context.DEFAULT_DIRERR)
  def dirout() = this.getOrElse(Context.DIROUT,Context.DEFAULT_DIROUT)
  def dirin() = this.getOrElse(Context.DIRIN,Context.DEFAULT_DIRIN)
  def control() = this.getOrElse(Context.CONTROL,Context.DEFAULT_CONTROL)
  def meta() = this.getOrElse(Context.META,Context.DEFAULT_META)

  def repo(repo: Repo=null):Repo = { if (repo != null) this.repo=repo; this.repo }

  override def toString() = {
    this.map(e=>e._1+": "+e._2).mkString("\n")
  }
}
