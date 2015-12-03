package com.alstom.datalab.exception

/**
  * Created by raphael on 03/12/2015.
  */
class MissingWebRequestException(filename: String) extends Exception {
  def file() = filename
}
