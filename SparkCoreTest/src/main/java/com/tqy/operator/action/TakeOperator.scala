package com.tqy.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TakeOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    //返回一个由RDD的前n个元素组成的数组
    println(rdd.take(3).mkString(","))

  }
}
