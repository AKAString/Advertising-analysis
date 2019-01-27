package com.qr.spark.useraction.dao

import scala.collection.mutable.ListBuffer
import com.qr.spark.useraction.jdbc.JDBCManager
import scala.collection.mutable.ArrayBuffer

trait Top10ClickDetailDao {
  def addTop10ClickDetailDao(sql: String, paramsList: ListBuffer[ArrayBuffer[Any]])={
    val jdbcManager=new JDBCManager()
    jdbcManager.executeBatch(sql, paramsList)
  }
}