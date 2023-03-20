package com.tqy.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByKeyOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 3), ("c", 2), ("d", 1), ("e", 4), ("b", 5))
    )

    //升序
//    val rdd2: RDD[(String, Int)] = rdd1.sortByKey()
    //降序
    val rdd2: RDD[(String, Int)] = rdd1.sortByKey(false)

    println(rdd2.collect().mkString(","))

  }

}
