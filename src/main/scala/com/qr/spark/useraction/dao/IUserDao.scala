package com.qr.spark.useraction.dao

import com.qr.spark.useraction.entity.BlackList
import com.qr.spark.useraction.jdbc.JDBCManager
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

trait IUserDao {
  def getBlackList() = {
    val rs = JDBCManager.jdbc.executeQuery("select * from ad_blacklist");
    val list = ListBuffer[BlackList]()
    while (rs.next()) {
      var black = new BlackList()
      black.userId = rs.getInt(1)
      list.+=(black)
    }
    list
  }
  def addBlack(userId: Any) = {
    val sql = "insert into ad_blacklist values(?) ON DUPLICATE KEY UPDATE user_id=?"
    val paramsList = ListBuffer[ArrayBuffer[Any]]()
    val array = new ArrayBuffer[Any]()
    array.+=(userId)
    array.+=(userId)
    paramsList.+=(array)
    JDBCManager.jdbc.executeBatch(sql, paramsList)
  }
}