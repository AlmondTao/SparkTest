package com.tqy.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc:SparkContext = new SparkContext(conf)

    val fileRdd = sc.textFile("input/wordcount.txt")

//    val flatList = fileRdd.flatMap(_.split(" "))
//    val groupMap = flatList.groupBy(word => word)
//    val result = groupMap.mapValues(_.size)
//    result.foreach(println(_))
//sc.textFile("input/wordcount.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println(_))
    val result = fileRdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.foreach(println(_))

    result.saveAsTextFile("out")

    sc.stop()

  }

}
