package com.qr.spark.useraction.dao

import com.qr.spark.useraction.entity.Ad
import com.qr.spark.useraction.jdbc.JDBCManager
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

trait AdDao {
  /*CREATE TABLE  ad_province_top3(
  date  varchar(30) ,
  province varchar(100) ,
  ad_id  int(1),
  click_count  int(11) DEFAULT NULL,
primary key(date,province )
) ENGINE=InnoDB DEFAULT CHARSET=utf8
*/
  def addAd(ad: ListBuffer[ArrayBuffer[Any]]) {
    var sql = "insert into ad_province_top3 values(?,?,?,?)"
   
    JDBCManager.jdbc.executeBatch(sql, ad);
  }
  def addAdTrend(ad: ListBuffer[ArrayBuffer[Any]]) {
    var sql = "insert into ad_province_top3 values(?,?,?,?)"
    JDBCManager.jdbc.executeBatch(sql, ad);
  }
  def deleteAd(array: ArrayBuffer[Any]) {
    var lb = ListBuffer[ArrayBuffer[Any]]()
    lb.+=(array)
    val sql = "delete from ad_province_top3 where date=? and province=?"
    JDBCManager.jdbc.executeBatch(sql, lb);
  }
  /*CREATE TABLE  ad_click_trend (
  date  varchar(30) ,
  ad_id  int(11) ,
  minute  varchar(30),
  click_count  int(11) DEFAULT NULL,
 primary key(date,ad_id,minute)
) ENGINE=InnoDB DEFAULT CHARSET=utf8*/
  def addTrend(ad: ListBuffer[ArrayBuffer[Any]]) ={
     val sql="insert into ad_click_trend values(?,?,?,?)"
      JDBCManager.jdbc.executeBatch(sql, ad);
  }
}