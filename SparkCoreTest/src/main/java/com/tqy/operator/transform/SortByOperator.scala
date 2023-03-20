package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortByOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(
      List(2, 1, 4, 3, 2, 7, 10, 13, 16, 8),3
    )

    val rdd1: RDD[Int] = rdd.sortBy(num => num, false)

    println(rdd1.collect().mkString(","))
  }
}
