package com

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SQLContext

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sessionAnalysis")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val rdd1 = sc.parallelize(List(("FFFDDSADFSFS1", "1"), ("FSFFDDSADFSFS2", "34")), 1)
    val j1 = rdd1.toDF("sid1", "times")
    val rdd2 = sc.parallelize(List(("FFFDDSADFSFS1", "2"), ("FSFFDDSADFSFS2", "12")), 1)
    val j2 = rdd2.toDF("sid2", "times")    
    j1.join(j2).where("sid1=sid2").show()

  }
}