package com.tqy.broadCast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 1), ("a", 2), ("c", 3), ("b",7))
    )


    val list1 = List(("b", 4), ("c", 5), ("c", 6))
    val bcList: Broadcast[List[(String, Int)]] = sc.broadcast(list1)

    val rdd2: RDD[(String, Int)] = rdd1.map(
      data => {
        var resultInt: Int = data._2
        val bcValue: List[(String, Int)] = bcList.value
        bcValue.foreach(
          bcData => {
            if (data._1 == bcData._1) {
              println("++++++++++")
              resultInt += bcData._2
            }
          }
        )
        (data._1, resultInt)
      }
    )

    println(rdd2.collect().mkString(","))


  }

}
