package com.tqy.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TakeOrderedOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(
      List(4, 6, 3, 1, 5, 2)
    )

    //返回一个由RDD的前n个元素组成的数组
    println(rdd.takeOrdered(4)(Ordering.Int.reverse).mkString(","))

  }
}
