package com

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestObjSortRDD {
  case class CPO(sid: String, c: Int, p: Int, o: Int)
  /*implicit def toCpo(cpo: CPO) = new Ordered[CPO]() {
    def compare(that: CPO) = {
      if (cpo.c == that.c) {
        if (cpo.p == that.p) {
          cpo.o - that.o
        } else {
          cpo.p - that.p
        }
      } else {
        cpo.c - that.c
      }
    }
  }*/
  implicit val to = new Ordering[CPO]() {
    def compare(cpo:CPO, that:CPO) = {
      if (cpo.c == that.c) {
        if (cpo.p == that.p) {
          cpo.o - that.o
        } else {
          cpo.p - that.p
        }
      } else {
        cpo.c - that.c
      }
    }
  }  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sessionAnalysis")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("s1", (2, 34, 5)), ("s1", (12, 34, 56)), ("s1", (11, 1, 56)), ("s1", (566, 3, 56)), ("s1", (23, 34, 6))))
    val rdd2 = rdd1.map(f => (CPO(f._1, f._2._1, f._2._2, f._2._3))).takeOrdered(3)
    rdd2.foreach { x => println(x) }

  }
}