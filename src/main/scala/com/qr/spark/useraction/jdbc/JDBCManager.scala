package com.qr.spark.useraction.jdbc

import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import java.sql.Connection
import java.sql.PreparedStatement
import scala.collection.mutable.LinkedList
import com.mysql.jdbc.Driver
import java.sql.DriverManager

class JDBCManager {

  def executeQuery(sql: String): ResultSet = {
    val con = JDBCManager.getConnection();
    val rs = con.prepareStatement(sql).executeQuery()
    rs
  }
  def executeBatch(sql: String, paramsList: ListBuffer[ArrayBuffer[Any]]) {
    var rtn: Array[Int] = null;
    if (paramsList.length >= 0) {
      var conn: Connection = null;
      var pstmt: PreparedStatement = null;
      try {
        conn = JDBCManager.getConnection();
        conn.setAutoCommit(false);
        pstmt = conn.prepareStatement(sql);
        if (paramsList != null)
          for (obj <- paramsList) {
            for (i <- 0.until(obj.length)) {
              pstmt.setObject(i + 1, obj(i));
            }
            pstmt.addBatch();
          }
        rtn = pstmt.executeBatch();
        conn.commit();
      } catch {
        case e: Exception => e.printStackTrace();
      } finally {
       conn.close()
      }
    }
    rtn
  }
}

object JDBCManager {
  val jdbc = new JDBCManager
  def instatnce(): JDBCManager = {
    jdbc
  }
  var con: Connection = null;
  def getConnection(): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    con = DriverManager.getConnection("jdbc:mysql://localhost:3306/advice?useUnicode=true&characterEncoding=utf8", "root", "root");
    con

  }
  def close() {
    con.close()
  }
}