package com.qr.spark.useraction

import org.apache.spark.AccumulatorParam
import scala.collection.mutable.Map
class SessionAccumulation extends AccumulatorParam[Map[String, Int]] {


  /*override
  def addAccumulator(initMap: Map[String, Int], newElem: Map[String, Int]): Map[String, Int] = {
     add1(initMap, newElem);
  }*/
  def addInPlace(initMap: Map[String, Int], newElem: Map[String, Int]): Map[String, Int] =
    {
      add1(initMap, newElem);
    }
  def add1(initMap: Map[String, Int], newElem: Map[String, Int]): Map[String, Int] = {
    if (initMap == null) {
      newElem
    } else {
      val key = newElem.keySet.iterator.next()
   
      initMap(key) = (initMap(key) + 1)
      initMap
    }
  }
  def zero(initMap: Map[String, Int]): Map[String, Int] = {
    Map("0s-30S" -> 0, "30s-60s" -> 0, "1m-3m" -> 0, "3m-10m" -> 0, "10m" -> 0, "0-4" -> 0, "4-7" -> 0, "7-10" -> 0,"sessionCount"->0)
  }
}