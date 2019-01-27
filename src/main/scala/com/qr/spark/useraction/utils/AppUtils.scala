package com.qr.spark.useraction.utils

import java.util.Properties
import java.io.FileInputStream

object AppUtils {
  val ISDEBUG = "isdebug"
  val props = new Properties()
  props.load(AppUtils.getClass.getResourceAsStream("/app.properties"))
  def get(key: String) = {
    props.getProperty(key)
  }
  def main(args: Array[String]): Unit = {
    AppUtils.get("isdebug")
  }
}