package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DistinctOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(
      List(4, 2, 3, 3, 4, 5, 2,5),2
    )

    implicit val reverse: Ordering[Int] = Ordering.Int
    val rdd1: RDD[Int] = rdd.distinct(2)(reverse)

    println(rdd1.collect().mkString(","))

  }
}
