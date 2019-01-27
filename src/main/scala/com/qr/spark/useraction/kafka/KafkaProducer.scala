package com.qr.spark.useraction.kafka

import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig
import java.util.Properties
import scala.util.Random
import kafka.producer.KeyedMessage
import kafka.producer.KeyedMessage
import kafka.producer.KeyedMessage
import kafka.producer.KeyedMessage

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    /* timestamp	1450702800
province 	henan	
city 	Nanjing
userid 	100001
adid 	100001*/

    val props = new Properties()
    props.put("zk.connect", "192.168.38.131:2181,192.168.38.132:2181,192.168.38.133:2181");
    props.put("metadata.broker.list", "192.168.38.131:9092,192.168.38.132:9092,192.168.38.133:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](producerConfig)
    
    val province = List("河南省", "河北省", "北京市", "上海市", "天津市")
    val citys = List(List("周口", "漯河", "洛阳", "新郑", "信阳", "驻马店"), List("邢台", "石家庄", "衡水", "燕郊", "沧州", "河北1", "河北2"), List("北京市1", "北京市2", "北京市3", "北京市4", "北京市5", "北京市6", "北京市7"), List("上海市1", "上海市2", "上海市3", "上海市4", "上海市5", "上海市6", "上海市7"), List("天津市1", "天津市2", "天津市3", "天津市4", "天津市5"))
    val random = new Random()
    var i=0
    while (true) {
      i=i+1
      val pro = random.nextInt(province.size);
      val prov = province(pro)//取省
      val cityRandom = citys(pro)//省下所有市
     // println(cityRandom)
      val cityVal=cityRandom(random.nextInt(cityRandom.size))//取某个市
      val userId = random.nextInt(100)
      val adiviceId = random.nextInt(100)
      val ms = System.currentTimeMillis()
      val key = prov + "," + cityVal + "," + userId + "," + adiviceId + "," + ms
      println(key)
      val message = new KeyedMessage[String, String]("ad", key)
      producer.send(message)
      Thread.sleep(1000)
    }
  }
}