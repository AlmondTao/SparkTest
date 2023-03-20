package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FlatMapOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[List[Int]] = sc.makeRDD(
      List(List(1, 2), List(3, 4, 5))
    )

    val rdd2: RDD[Int] = rdd.flatMap(
      list => list
    )

    rdd2.foreach(println(_))



  }
}
