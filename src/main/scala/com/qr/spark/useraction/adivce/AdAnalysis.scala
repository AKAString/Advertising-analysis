package com.qr.spark.useraction.adivce

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import com.qr.spark.useraction.utils.DateUtils
import cn.itcast.spark.utils.LoggerLevels
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import com.qr.spark.useraction.dao.impl.DAOFactory
import com.mysql.jdbc.Driver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import com.qr.spark.useraction.entity.Ad
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.Minutes
import java.text.SimpleDateFormat

object AdAnalysis {
  def main(args: Array[String]): Unit =
    { 
      LoggerLevels.setStreamingLogLevels()
      val conf = new SparkConf()
      conf.setAppName("adivce3")
      conf.setMaster("local[2]")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(5))
      ssc.checkpoint("/111/out32")
      /*val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.38.104:9092,192.168.38.105:9092,192.168.38.106:9092")
      val streaming = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("advice"))*/
      val topics = Map("ad" -> 1)
      //从kafka读取数据
      val streaming = KafkaUtils.createStream(ssc, "192.168.38.131:2181,192.168.38.132:2181,192.168.38.133:2181", "ad", topics, StorageLevel.MEMORY_AND_DISK)
      //======================基于黑名单的非法广告点击流量过滤机制  开始=================================
      //DStreaming(map,flatMap)
      //transform能够让程员获取到 RDD,同时对rdd(RDD里面有好多方法)进行处理(可以生成一个新的RDD做后续的处理，赋予transform权利)
        val stream = streaming.transform(rdd => {     
        val blackList = DAOFactory.newUserDao().getBlackList()
        val blackListMap = sc.parallelize(blackList)
        //map->一对一转换(用户编号，false)
        val userBlckList = blackListMap.map { x => (x.userId.toString(), false) }
        //天津市,天津市2,9,43,1545552355516
        //info装的是(用户编号,整条信息)
        val info = rdd.map(f => {
          println(f._2)//取第2个(value)
          val info = f._2.split(",")
          (info(2), f._2)
        }) 
        //rddAll(用户编号,(整条信息,false|None))
        val rddAll = info.leftOuterJoin(userBlckList)
        val rdd2 = rddAll.filter(f => f._2._2 == None)
        //用户编号  整条信息
        rdd2.map(f => (f._1, f._2._1))
        
      })
      //======================基于黑名单的非法广告点击流量过滤机制 结束 =================================
      //天津市,天津市2,9,43,1545552355516
     /* val initStream = streaming.map(f => {
        println(f._2)
        val info = f._2.split(",")
        val date = DateUtils.toDate(info(4).toLong)
        //省                                                                                市                                                                          用户编号                                                                    广告编号                                                        日期
        (info(0).toString() + "," + info(1).toString() + "," + info(2).toString() + "," + info(3).toString() + "," + date, 1)
      })*/

      //====================统计每天各省top3热门广告 开始==============================
      //天津市,天津市2,9,43,1545552355516
      val proviceDate = streaming.map(x => {
        val advice = x._2.split(",")
        //日期                                                                                省                             广告编号
        (DateUtils.toDate(advice(4).toLong) + "," + advice(0) + "," + advice(3), 1)
      })
       //(日期,省,广告编号    次数)
      //updateStateByKey(reduceByKey(过来一次聚合一次，不具备持久保存的功能)) 一直聚合       x:Seq[Int]  y是累加值(以前累加值)
        val provinceByDate = proviceDate.updateStateByKey((x, y: Option[Int]) => {
        Some(x.sum + y.getOrElse(0))
      })
      
      case class City(city: String, times: Int)
      
      val provinceByDateToCityCount = provinceByDate.map(x => {
        val key = x._1.split(",")
        //日期                                    省                                   广告             次数 二次分组
        (key(0) + "," + key(1), City(key(2), x._2))
      })
      //combineBykey(aggreageByKey)
      val proviceAdviseTop3 = provinceByDateToCityCount.combineByKey(x => List(x), (a: List[City], b: City) => a.+:(b), (c: List[City], b: List[City]) => {
      //累加       
       var l = c.++(b);
        //按照次数排序取Top3
        val ll = l.sortBy { x => -x.times }
        ll.take(3)
      }, new HashPartitioner(1), true)
      //存入数据库
      proviceAdviseTop3.foreachRDD(rdd => {
        rdd.foreachPartition(f => {
          while (f.hasNext) {
           //日期                                    省                                  City( 广告             次数) 二次分组
            val provice = f.next();
                               //日期             省
            val provinceDate = provice._1.split(",")
            val ar = new ArrayBuffer[Any]()
            ar.+=(provinceDate(0))
            ar.+=(provinceDate(1))
            //先删除原来的前日期 前三
            DAOFactory.newAdDao().deleteAd(ar);
            
            var lb = ListBuffer[ArrayBuffer[Any]]()
            for (c <- provice._2) {
              var array = new ArrayBuffer[Any]()
              array.+=(provinceDate(0))//日期
              array.+=(provinceDate(1))//省
              array.+=(c.city)//城市
              array.+=(c.times)//次数
              lb.+=(array)
            }
            DAOFactory.newAdDao().addAd(lb)

          }

        })

      })
      //====================统计每天各省top3热门广告 结束==============================

      //========================各广告最近1小时内各分钟的点击量 start===============================
      val trendencyMap = streaming.map(f => {
        //天津市,天津市2,9,43,1545552355516
        val info = f._2.split(",")
        //转换成日期
        val date = DateUtils.toDate(info(4).toLong)
        //用户编号                                                           日期
        (info( 3).toString() + "," + date, 1)
      })
      //窗口函数reduceByKeyAndWindow
      //reduceByKeyAndWindow也是reduceByKey,但与reduceByKey有区别,可以设置保存时间Minutes(60),Minutes(1)表示多久计算一次
      //统计各广告最近1小时内的点击量趋势：各广告最近1小时内各分钟的点击量
      val trendency = trendencyMap.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Minutes(60), Minutes(1))
      //保存数据库
      trendency.foreach(rdd => {
        rdd.foreachPartition(it => {
          while (it.hasNext) {
            val elm = it.next()
            val elms = elm._1.split(",")
            var adid = elms(0)
            var addate = elms(1)
            val adtimes = elm._2
            val simeDateFormat = new SimpleDateFormat("mm")
            val minute = simeDateFormat.format(new java.util.Date())
            println(adid, addate, adtimes, minute)
            val lst = new ListBuffer[ArrayBuffer[Any]]()
            val array = new ArrayBuffer[Any]();
            array.+=(addate)
            array.+=(adid)
            array.+=(minute)
            array.+=(adtimes)
            lst.+=(array);
            println(array.toBuffer + "  ====================================")
            DAOFactory.newAdDao().addTrend(lst);
          }
        })
      })
      //========================各广告最近1小时内各分钟的点击量 end===============================
      //===========================各广告省市单击次数 开始============================================
      //天津市,天津市2,9,43,1545552355516
      /*CREATE TABLE  ad_stat (
  date varchar(30),
  province varchar(100) ,
  city  varchar(100),
  ad_id  int(11),
  click_count int(11) DEFAULT NULL,
  primary key(date,province,city,,ad_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
*/
      ////天津市,天津市2,9,43,1545552355516
      val provinceCityAdOne = streaming.map(x => {
        val sp = x._2.split(",")
        //省                                      市                                   广告编号                   日期           1
        (sp(0) + "," + sp(1) + "," + sp(3)+","+DateUtils.toDate(sp(4).toLong), 1)
      })
      val provinceCityAd = provinceCityAdOne.updateStateByKey((x: Seq[Int], y: Option[Int]) => {
        Some(x.sum + y.getOrElse(0))
      })      
       provinceCityAd.foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          var con: Connection = null
          var ps: PreparedStatement = null         
          Class.forName("com.mysql.jdbc.Driver")
          con = DriverManager.getConnection("jdbc:mysql://localhost:3306/advice?useUnicode=true&characterEncoding=utf8", "root", "root");
          ps = con.prepareStatement("insert into ad_stat values(?,?,?,?,?) ON DUPLICATE KEY UPDATE click_count=?");
         while (it.hasNext) {
            val elm = it.next()
            //println(elm._1 + ":ddd")
            val info = elm._1.split(",")
            ps.setString(1, info(3).toString())
            ps.setString(2, info(0).toString())
            ps.setString(3, info(1).toString())
            ps.setString(4, info(2).toString())
            //ps.setString(5, info(2).toString())
            ps.setInt(5, elm._2.toInt)
            ps.setInt(6, elm._2.toInt)
            ps.executeUpdate()
           
          }         
          ps.close()
          con.close()
        })

      })
     // ===========================各广告省市单击次数 结束============================================
      
     // ====================================用户各广告单击次数 开始===================================
      /*CREATE TABLE ad_user_click_count(
  date varchar(30) ,
  user_id int(11) ,
  ad_id  int(11) ,
  click_count int(11) DEFAULT NULL,
primary key(date,user_id,ad_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
select user_id from ad_user_click_count where click_count>=100;*/
      //天津市,天津市2,9,43,1545552355516
       val userAdMap = streaming.map(x => {
        val sp = x._2.split(",")
        //日期                     用户编号          广告编号        1
        (DateUtils.toDate(sp(4).toLong)+","+sp(2)+","+sp(3), 1)
      })
      val proCityDate = userAdMap.updateStateByKey((x, y: Option[Int]) => { //x有多个1
        Some(x.sum + y.getOrElse(0))
      })
      proCityDate.foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          var con: Connection = null

          var ps1: PreparedStatement = null
          var ps4: PreparedStatement = null
          Class.forName("com.mysql.jdbc.Driver")
          con = DriverManager.getConnection("jdbc:mysql://localhost:3306/advice?useUnicode=true&characterEncoding=utf8", "root", "root");
          ps1 = con.prepareStatement("insert into ad_user_click_count values(?,?,?,?) ON DUPLICATE KEY UPDATE click_count=?");         
          ps4 = con.prepareStatement("select user_id from ad _user_click_count where click_count>=100");
          while (it.hasNext) {
            val elm = it.next()
            //println(elm._1 + ":ddd")
            val info = elm._1.split(",")
            ps1.setString(1, info(0).toString())
            ps1.setString(2, info(1).toString())
            ps1.setString(3, info(2).toString())
            ps1.setInt(4, elm._2.toInt)
            ps1.setInt(5, elm._2.toInt)
            ps1.executeUpdate()
          }
          val rs = ps4.executeQuery();
          while (rs.next()) {
            val userId = rs.getInt(1)
            DAOFactory.newUserDao().addBlack(userId);
          }
          rs.close()
          
          ps1.close()
          ps4.close()
          con.close()
        })

      })
    // ====================================用户各广告单击次数 结束===================================
      ssc.start()
      ssc.awaitTermination()

    }
}