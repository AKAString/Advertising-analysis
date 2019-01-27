package com.qr.spark.useraction.session

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import cn.itcast.spark.utils.LoggerLevels
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Accumulator
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

import com.qr.spark.useraction.dao.impl.DAOFactory
import com.qr.spark.useraction.utils.DateUtils
import com.qr.spark.useraction.SessionAccumulation
import com.qr.spark.useraction.utils.AppUtils
import com.qr.spark.useraction.mock.UserMock

object SessionAnalysis 
{
  def getSqlContext(sc: SparkContext) = {
    if (AppUtils.get(AppUtils.ISDEBUG).toBoolean) {
      val sqlContext = new SQLContext(sc)
      UserMock.mock(sc, sqlContext)
      sqlContext
    } else {
      new HiveContext(sc);
    }
  }
  def getSessionsByDateRange(sqlContext: SQLContext, start: String, end: String) = {
    sqlContext.sql("select uv.* from userVisit uv,userInfo ui where uv.actionTime > '" + start + "' and uv.actionTime < '" + end + "' and uv.userId=ui.userId");
  }
  
  def secondStep(s: Long) =
    {
      var res = ""
      //"0s-30S" -> 0, "30s-60s" -> 0, "1m-3m" -> 0, "3m-10m" -> 0, "10m" -> 0, "0-4" -> 0, "4-7" -> 0, "7-10" -> 0
      if (s < 30) {
        res = "0s-30S"
      }
      if (s >= 30 & s < 60) {
        res = "30s-60s"
      }
      if (s >= 60 & s < 180) {
        res = "1m-3m"
      }
      if (s >= 180 & s < 600) {
        res = "3m-10m"
      }
      if (s >= 600) {
        res = "10m"
      }
      String.valueOf(res)
    }
  
  def pageCountStep(s: Int) =
    {
      var res = ""
      //"0s-30S" -> 0, "30s-60s" -> 0, "1m-3m" -> 0, "3m-10m" -> 0, "10m" -> 0, "0-4" -> 0, "4-7" -> 0, "7-10" -> 0
      if (s < 4) {
        res = "0-4"
      }
      if (s >= 4 & s < 7) {
        res = "4-7"
      }
      if (s >= 7 & s <= 10) {
        res = "7-10"
      }
      res
    }
  def toSecond(ms: Long) = ms / 1000
  def computeStepAndSessionRange(sessionsDateRange: DataFrame, acc_broad: Broadcast[Accumulator[Map[String, Int]]]) =
    {
      //以session为key,求时间最大值 和最小值
      //以session为key，求pageid个数    
      //date, userId, sessionId, actionTime, action, payCategoryId, payProductId, clickCategoryId, clickProductId, orderCategoryId, orderProductId, keywords
      //                                     sessionid        actiontime
      val map1 = sessionsDateRange.map { x => (x(2).toString(), x(3).toString()) }
      val mapMinMax = map1.reduceByKey((x, y) => { //x是累加值  y是被加值
        val ar = x.split(",")
        if (ar.length == 1) {
          val tmp = ar(0).compareTo(y)
          if (tmp >= 0) ar(0) + "," + y else y + "," + ar(0) //大值，小值
        } else {
          var max = ""
          var min = ""
          var tmp = ar(0).compareTo(y)
          if (tmp >= 0) max = ar(0) else max = y
          tmp = ar(1).compareTo(y)
          if (tmp >= 0) min = y else min = ar(1)
          max + "," + min
        }
      })
      //sessionid                  pageid
      val map2 = sessionsDateRange.map { x => (x(2).toString(), x(12).toString()) }
      val map3 = map2.combineByKey(x => Set(x), (a: Set[String], b: String) => a.+(b), (c: Set[String], d: Set[String]) => c ++ d)
      //                           sessionid  pageidcount
      val mapPageNum = map3.map(f => (f._1, f._2.size))

      val minMaxPageNumJoin = mapMinMax.join(mapPageNum)

      minMaxPageNumJoin.filter(f => f._2._2 != 1).map(f => {
        val minMax = f._2._1.split(",");
        val ms = toSecond(DateUtils.toYYYYMMDDHHMMSS(minMax(0)) - DateUtils.toYYYYMMDDHHMMSS(minMax(1)))
        val mm = secondStep(ms)
        acc_broad.value.add(Map(mm -> 1))
        acc_broad.value.add(Map(pageCountStep(f._2._2) -> 1))
        acc_broad.value.add(Map("sessionCount" -> 1))
        //sessionid maxdate mindate   pageidcount
        (f._1, minMax(1), minMax(0), f._2._2)
      })
    }
  //
  def clickTop10(sc: SparkContext, top10Click: Array[(String, Int)], df: DataFrame) = {
    //                   categoryid         clickcount
    val rdd1 = sc.parallelize(top10Click)
    //                   clickcategoryid    sessionid 
    val rdd2 = df.select("clickcategoryid", "sessionId").where("clickcategoryid!=0").map { x => (x(0).toString(), x(1).toString()) }
    //                      sessionid  categroyid  clickcount
    rdd1.join(rdd2).map(f => (f._2._2, (f._1, f._2._1)))
  }
  case class Category(categoryId: String, click: Int, order: Int, pay: Int)
  implicit def toCategory(cat: Category) = new Ordered[Category] {
    def compare(that: Category) = {
      if (cat.click == that.click) {
        if (cat.order == that.order) {
          that.pay - cat.pay
        } else {
          that.order - cat.order
        }
      } else {
        that.click - cat.click
      }
    }
  }
  //rdds符合maxage minage之后记录
  def getClickOrderPayTop10(rdds: DataFrame) = {
    //获取支付种类id 支付次数
     val pay = rdds.select("payCategoryId").where("payCategoryId!=0").map { x => (x(0).toString(), 1) }.reduceByKey(_ + _);
   // val pay = rdds.select("payCategoryId", "action").filter(rdds.col("action").equalTo("pay") && rdds.col("payCategoryId") != 0).map { x => (x(0).toString(), 1) }.reduceByKey(_ + _);
    //获取单击种类id 次数
    val click = rdds.select("clickcategoryid", "action").filter(rdds.col("action").equalTo("click")).map { x => (x(0).toString(), 1) }.reduceByKey(_ + _);
    //下单种类id  次数
    val order = rdds.select("orderCategoryId", "action").filter(rdds.col("action").equalTo("order")).map { x => (x(0).toString(), 1) }.reduceByKey(_ + _);
    //内连接 获取种类 单击次数 下单次数  支付次数 top10
    val clickOrderPay = click.join(order).join(pay).map(f => (Category(f._1, f._2._1._1, f._2._1._2, f._2._2))).takeOrdered(10)
    
    //获取单击种类编号  单击次数  top10
    val topClick = clickOrderPay.map { x => (x.categoryId, x.click) }

    (clickOrderPay, topClick)
  }
  
  def addStepToDb(m: Map[String, Int]) = {
    println(m.toBuffer)
  }

  //session_random_extract
  /**
   * CREATE TABLE  session_random_extract(
   * task_id  int(11) NOT NULL,
   * session_id  varchar(255) DEFAULT NULL,
   * start_time  varchar(50) DEFAULT NULL,
   * end_time varchar(50) DEFAULT NULL
   * ) ENGINE=InnoDB DEFAULT CHARSET=utf8
   * //session_random_extract
   * ////sessionid (1,(maxdate,mindate))
   */
  def sessionRandomExtract(randomSessionList: RDD[(String, (Int, (String, String)))]) = {
    randomSessionList.foreachPartition(it => {
      while (it.hasNext) {
        //(234d4ee4-6a79-46c2-a014-27fd43507719,(1,(2019-01-13 01:01:28,2019-01-13 01:59:04)))
        val next = it.next()
        val sessionid = next._1
        val maxdate = next._2._2._1
        val mindate = next._2._2._2
        val sql = "insert into session_random_extract values(1,?,?,?)"
        val paramsList = new ListBuffer[ArrayBuffer[Any]]()
        val buffer = new ArrayBuffer[Any]()
        buffer.+=(sessionid)
        buffer.+=(maxdate)
        buffer.+=(mindate)
        paramsList.+=(buffer)
        DAOFactory.newSessionRanom_extractDaoImp.addSession_random_extract(sql, paramsList)
      }
    })
  }
  //top10详细session                           sessionid categroyid clickcount
  def top10SessionDetail(top10ClickSession: RDD[(String, (String, Int))], df: DataFrame) = {
    val top10 = top10ClickSession.map(f => (f._1, f._2._1)).reduceByKey(_ + _)
    val rdd1 = df.rdd.map { x => (x(2).toString(), x) }
    val rdd2 = top10.join(rdd1).map(f => f._2._2)
    rdd2
  }
  /**
   * CREATE TABLE `top10_category` (
   * `task_id` int(11) NOT NULL,
   * `category_id` int(11) DEFAULT NULL,
   * `click_count` int(11) DEFAULT NULL,
   * `order_count` int(11) DEFAULT NULL,
   * `pay_count` int(11) DEFAULT NULL
   * ) ENGINE=InnoDB DEFAULT CHARSET=utf8
   *
   */
  def top10ByClickOrderyPayDB(top10ByClickOrderyPay: Array[Category]) = {
    top10ByClickOrderyPay.map { x =>
      {
        val sql = "insert into top10_category values(1,?,?,?,?)"
        val paramsList = new ListBuffer[ArrayBuffer[Any]]()
        val buffer = new ArrayBuffer[Any]()
        buffer.+=(x.categoryId)
        buffer.+=(x.click)
        buffer.+=(x.order)
        buffer.+=(x.pay)
        paramsList.+=(buffer)
        DAOFactory.newClickOrderyPayDaoImp.addClickOrderyPay(sql, paramsList)

      }
    }
  }
 /*
  *  CREATE TABLE `top10_category_session` (
  `task_id` int(11) NOt NULL,
  `category_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8
  
*/
  //sessionid  categroyid  clickcount
  def top10ClickSessionDB(top10ClickSession: RDD[(String, (String, Int))])={
    top10ClickSession.foreachPartition(it=>{
      while(it.hasNext)
      {
        val elm=it.next()
         val sql = "insert into top10_category_session values(1,?,?,?)"
        val paramsList = new ListBuffer[ArrayBuffer[Any]]()
        val buffer = new ArrayBuffer[Any]()
        buffer.+=(elm._2._1)
        buffer.+=(elm._1)
        buffer.+=(elm._2._2)
        paramsList.+=(buffer)
        DAOFactory.newTop10ClickSessionDaoImp().addTop10ClickSessionDao(sql, paramsList)
      }
    })
   
  }
  
  /**
   * CREATE TABLE `session_detail` (
  `task_id` int(11) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `page_id` int(11) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,  
  `search_keyword` varchar(255) DEFAULT NULL,  
  `click_category_id` int(11) DEFAULT NULL,  
  `click_product_id` int(11) DEFAULT NULL,  
  `order_category_ids` varchar(255) DEFAULT NULL,
  
  `order_product_ids` varchar(255) DEFAULT NULL,
  `pay_category_ids` varchar(255) DEFAULT NULL,
  `pay_product_ids` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
   * date, userId, sessionId, actionTime, action, payCategoryId, payProductId, clickCategoryId, 
   * clickProductId, orderCategoryId, orderProductId, keywords, pageId
   */
  def top10ClickDetailDB(top10ClickDetail: RDD[Row])={
    top10ClickDetail.foreachPartition { x => {
      while(x.hasNext)
      {
        val row=x.next() 
         val sql = "insert into session_detail values(1,?,?,?,?,?,?,?,?,?,?,?)"
        val paramsList = new ListBuffer[ArrayBuffer[Any]]()
        val buffer = new ArrayBuffer[Any]()
        buffer.+=(row(1))
        buffer.+=(row(2))
        buffer.+=(row(12))
        buffer.+=(row(3))
        buffer.+=(row(11))        
        buffer.+=(row(7))        
        buffer.+=(row(8))        
        buffer.+=(row(9))        
        buffer.+=(row(10))        
        buffer.+=(row(5))
        buffer.+=(row(6))
        paramsList.+=(buffer)        
        DAOFactory.newTop10ClickDetailDaoImp().addTop10ClickDetailDao(sql, paramsList)
      }
    } }
  }
  
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val start = "2019-01-14 01:00"
    val end = "2019-01-14 05:00"
    val conf = new SparkConf()
    conf.setAppName("sessionAnalysis")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = getSqlContext(sc)    
    //======================1.按条件筛选session 开始===================================
    val sessionsDateRange = getSessionsByDateRange(sqlContext, start, end)
    val rawDataFrameByContation = sessionsDateRange.persist(StorageLevel.MEMORY_AND_DISK)
    //======================1.按条件筛选session 结束===================================

    //==================2、聚合统计：统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围
    var a = new SessionAccumulation()
    var acc = sc.accumulator(Map("0s-30S" -> 0, "30s-60s" -> 0, "1m-3m" -> 0, "3m-10m" -> 0, "10m" -> 0, "0-4" -> 0, "4-7" -> 0, "7-10" -> 0, "sessionCount" -> 0))(a)
    var acc_broad = sc.broadcast(acc)
    //sessionid,maxdate,mindate,pagecount
    val maxMinPageCount = computeStepAndSessionRange(rawDataFrameByContation, acc_broad)
    println(maxMinPageCount.count())
    //添加数据库
    addStepToDb(acc_broad.value.value)
    //==================2、聚合统计：结束========================================

    //==================3、在符合条件的session中，按照时间比例随机抽取1000个session(我这里30) 开始====
    val count = acc_broad.value.value.getOrElse("sessionCount", 1)//从上一步获取总session个数
    //hh 3 50                                    //2019-12-12 09             sessionid
    val sessionToDate = maxMinPageCount.map(f => (DateUtils.toYYYYMMDDHH(f._2), f._1))
    
    //小时数     List(sessionid,s2,s3)
    val listSession = sessionToDate.combineByKey((x: String) => List[String](x), (a: List[String], b: String) => a.+:(b), (c: List[String], d: List[String]) => c.++(d))
    ////小时数     List(sessionid,s2,s3) sessionid个数少了 
    val randomSessionListDate = listSession.map(f => {
      var list = ListBuffer[String]()
      var c = (f._2.size.toFloat / count) * 30
      val fetchCount = Math.ceil(c)
      for (i <- 0.until(fetchCount.toInt)) {
        list.+=(f._2(i))
      }
      (f._1, list)
    })
    val radnomSessionList = randomSessionListDate.map(f => f._2) //sessionid
    val rddRandomSessionList = radnomSessionList.flatMap { x => x } //sessionid    
    val rddSessionPair = rddRandomSessionList.map { x => (x, 1) } //sessionid  1 
    //                                                      sessionid maxdate mindate
    val rddSessionMaxDateMinDate = maxMinPageCount.map(f => (f._1, (f._2, f._3)))
    //sessionid (1,(maxdate,mindate))
    val randomSessionList = rddSessionPair.join(rddSessionMaxDateMinDate)
    randomSessionList.count()
    //把随即session信息写入数据库
    sessionRandomExtract(randomSessionList)
    //==================3、在符合条件的session中，按照时间比例随机抽取1000个session(我这里30) 结束====
    
    //=======================top10_category表，存储按点击、下单和支付排序出来的top10品类数据 开始==================
    val (top10ByClickOrderyPay, top10Click) = getClickOrderPayTop10(rawDataFrameByContation)
   //种类编号  单击次数 支付次数 下单次数
    top10ByClickOrderyPayDB(top10ByClickOrderyPay)
    //=======================top10_category表，存储按点击、下单和支付排序出来的top10品类数据 结束==================

    //======================对于排名前10的品类，分别获取其点击次数排名前10的session 开始==============================
    // sessionid  categroyid  clickcount
    val top10ClickSession = clickTop10(sc, top10Click, rawDataFrameByContation)
    top10ClickSessionDB(top10ClickSession)
    //session详细
    val top10ClickDetail = top10SessionDetail(top10ClickSession, rawDataFrameByContation)
    top10ClickDetailDB(top10ClickDetail)
    //======================对于排名前10的品类，分别获取其点击次数排名前10的session 结束==============================
    sc.stop()
  }
}