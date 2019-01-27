package com.qr.spark.useraction.dao

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import com.qr.spark.useraction.jdbc.JDBCManager

trait ClickOrderyPayDao {
  def addClickOrderyPay(sql: String, paramsList: ListBuffer[ArrayBuffer[Any]])={
     val jdbcManager=new JDBCManager()
    jdbcManager.executeBatch(sql, paramsList)
  }
}