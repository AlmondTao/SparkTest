package com.tqy.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CollectOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    //在驱动程序（Driver）中，以数组Array的形式返回数据集的所有元素
    println(rdd.collect().mkString(","))

  }
}
