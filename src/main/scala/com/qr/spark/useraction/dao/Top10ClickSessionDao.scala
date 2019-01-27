package com.qr.spark.useraction.dao

import scala.collection.mutable.ListBuffer
import com.qr.spark.useraction.jdbc.JDBCManager
import scala.collection.mutable.ArrayBuffer

trait Top10ClickSessionDao {
   def addTop10ClickSessionDao(sql: String, paramsList: ListBuffer[ArrayBuffer[Any]])={
    val jdbcManager=new JDBCManager()
    jdbcManager.executeBatch(sql, paramsList)
  }
}