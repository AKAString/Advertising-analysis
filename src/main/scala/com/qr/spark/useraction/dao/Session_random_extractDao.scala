package com.qr.spark.useraction.dao

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import com.qr.spark.useraction.jdbc.JDBCManager

trait Session_random_extractDao {
  def addSession_random_extract(sql: String, paramsList: ListBuffer[ArrayBuffer[Any]])={
    val jdbcManager=new JDBCManager()
    jdbcManager.executeBatch(sql, paramsList)
  }
}