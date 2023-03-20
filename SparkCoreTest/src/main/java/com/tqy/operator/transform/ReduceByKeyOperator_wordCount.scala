package com.tqy.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyOperator_wordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("input/wordcount.txt")
    val rdd1: RDD[String] = rdd.flatMap(
      _.split(" ")
    )
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)

    println(rdd3.collect().mkString(","))

  }

}
