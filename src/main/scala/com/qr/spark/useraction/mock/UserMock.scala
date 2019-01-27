package com.qr.spark.useraction.mock
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.qr.spark.useraction.utils.DateUtils
import scala.util.Random
import java.util.UUID
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.SparkConf
import java.io.FileWriter
object UserMock {
  
  // date userid sessionid,actiontime,action,paycategoryid,payproductid,clickcategoryid,clickproductid,ordercategoryid,orderproductid,searchkeys 
  def mock(sc: SparkContext, sql: SQLContext) {
    val fileWriter1 = new FileWriter("userVisit.txt", true)
    val fileWriter2 = new FileWriter("userInfo.txt", true)
    val date = DateUtils.toDate();
    val actions = Array("click", "pay", "order", "search")
    val searchKeys = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val rows = new ListBuffer[Row]()
    val random = new Random()
    for (i <- 0.until(100)) {
      val userId = random.nextInt(100)
      //println(userId)
      for (j <- 0.until(10)) {
        val baseDateTime = date + " " + DateUtils.numberFormat(random.nextInt(23))
        //println(baseDateTime)
        val sessionId = UUID.randomUUID().toString()
        for (f <- 0.until(random.nextInt(100))) {
          val pageId = random.nextInt(10)
          val actionTime = baseDateTime + ":" + DateUtils.numberFormat(random.nextInt(60)) + ":" + DateUtils.numberFormat(random.nextInt(60))
          val action = actions(random.nextInt(actions.length))
          var payCategoryId = 0
          var payProductId = 0
          var clickCategoryId = 0
          var clickProductId = 0
          var keywords = ""
          var orderCategoryId = 0
          var orderProductId = 0
          if (action.equals("click")) {
            clickCategoryId = random.nextInt(100)
            clickProductId = random.nextInt(100)
          }
          if (action.equals("pay")) {
            payCategoryId = random.nextInt(100)
            payProductId = random.nextInt(100)
          }
          if (action.equals("order")) {
            orderCategoryId = random.nextInt(100)
            orderProductId = random.nextInt(100)
          }
          if (action.equals("search")) {
            keywords = searchKeys(random.nextInt(searchKeys.length))
          }
          // date userid sessionid,actiontime,action,paycategoryid,payproductid,clickcategoryid,clickproductid,ordercategoryid,orderproductid,searchkeys 
          //println(date, userId, sessionId, actionTime, action, payCategoryId, payProductId, clickCategoryId, clickProductId, orderCategoryId, orderProductId, keywords)
          rows.+=(Row(date, userId, sessionId, actionTime, action, payCategoryId, payProductId, clickCategoryId, clickProductId, orderCategoryId, orderProductId, keywords, pageId));
          fileWriter1.write(date + "," + userId + "," + sessionId + "," + actionTime + "," + action + "," + payCategoryId + "," + payProductId + "," + clickCategoryId + "," + clickProductId + "," + orderCategoryId + "," + orderProductId + "," + keywords + "," + pageId+"\r\n");
        }
      }
     
    }
     fileWriter1.close();
    val schema = StructType(Array(StructField("date", StringType, true), StructField("userId", IntegerType, true),
      StructField("sessionId", StringType, true), StructField("actionTime", StringType, true), StructField("action", StringType, true),
      StructField("payCategoryId", IntegerType, true), StructField("payProductId", IntegerType, true),
      StructField("clickcategoryid", IntegerType, true), StructField("clickProductId", IntegerType, true),
      StructField("orderCategoryId", IntegerType, true), StructField("orderProductId", IntegerType, true), StructField("keywords", StringType, true), StructField("pageId", IntegerType, true)))
    val rdd = sc.parallelize(rows, 2)
    val dataFrame = sql.createDataFrame(rdd, schema)
    dataFrame.registerTempTable("userVisit");
    val users = ListBuffer[Row]()
    println("=========================================")
    for (i <- 0.until(100)) {
      //println(i)
      val userId = i;
      val userName = "name" + i;
      val userSex = "sex" + i;
      val userCity = i
      val professional = "professional" + i;
      val userAge = random.nextInt(60)
      users.+=:(Row(userId, userName, userSex, userCity, professional, userAge))
      fileWriter2.write(userId+","+ userName+","+userSex+","+userCity+","+professional+","+userAge+"\r\n");
    }
    fileWriter2.close();
    val userSchemal = StructType(Array(StructField("userId", IntegerType, true), StructField("userName", StringType, true), StructField("userSex", StringType, true), StructField("userCity", IntegerType, true), StructField("professional", StringType, true), StructField("userAge", IntegerType, true)))
    val userRdd = sc.parallelize(users)
    val userDF = sql.createDataFrame(userRdd, userSchemal)
    userDF.registerTempTable("userInfo")
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("user")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    mock(sc, new SQLContext(sc))
  }
}