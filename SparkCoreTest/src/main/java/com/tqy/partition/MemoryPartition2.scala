package com.tqy.partition

import org.apache.spark.{SparkConf, SparkContext}

object MemoryPartition2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc:SparkContext = new SparkContext(sparkConf)

    //(0 until numSlices).iterator.map { i =>
    //        val start = ((i * length) / numSlices).toInt
    //        val end = (((i + 1) * length) / numSlices).toInt
    //        (start, end)
    //      }

    //[0,3):【123】
    //[3,6):【456】
//    val rdd = sc.makeRDD(
//      List("1", "2", "3", "4", "5", "6"),2
//    )


    //[0,2):【12】
    //[2,5):【345】
//    val rdd = sc.makeRDD(
//      List("1", "2", "3", "4", "5"),2
//    )

    //[0,1):【1】
    //[1,3):【23】
    //[3,5):【45】
    val rdd = sc.makeRDD(
      List("1", "2", "3", "4", "5"),3
    )

    rdd.saveAsTextFile("output")
  }

}
