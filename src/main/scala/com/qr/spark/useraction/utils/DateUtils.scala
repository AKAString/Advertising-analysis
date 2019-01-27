package com.qr.spark.useraction.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.text.NumberFormat
import java.text.DecimalFormat
object DateUtils {
  private val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  private val DATE_FORMAT_YMDHMS = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  private val DATE_FORMAT_YMDHH = new SimpleDateFormat("yyyy-MM-dd hh")
  private val NUMBER_FORMAT = new DecimalFormat("00")
  def toDate() =
    {
      DATE_FORMAT.format(new Date())
    }
  def toDate(ms:Long) =
    {
      DATE_FORMAT.format(new Date(ms))
    }

  def numberFormat(v: Int) =
    {
      NUMBER_FORMAT.format(v)
    }
  def toYYYYMMDDHHMMSS(s:String)={
    DATE_FORMAT_YMDHMS.parse(s).getTime
  }
   def toYYYYMMDDHH(s:String)={
   DATE_FORMAT_YMDHH.format(DATE_FORMAT_YMDHMS.parse(s))
  }
  def main(args: Array[String]): Unit = {
   
    
    println(toYYYYMMDDHHMMSS("2018-12-21 03:59:25")- toYYYYMMDDHHMMSS("2018-12-21 03:02:40"))
  }
  
}