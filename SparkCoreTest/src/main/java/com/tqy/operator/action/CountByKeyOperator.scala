package com.tqy.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CountByKeyOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 2), ("b", 2), ("c", 3), ("a", 3), ("c", 4), ("a", 2))
    )

    val result: collection.Map[String, Long] = rdd.countByKey()
    val result2: collection.Map[(String, Int), Long] = rdd.countByValue()
    println(result)
    println(result2)

  }
}
