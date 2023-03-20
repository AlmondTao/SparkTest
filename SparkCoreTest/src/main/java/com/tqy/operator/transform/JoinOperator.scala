package com.tqy.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinOperator {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 1), ("a", 2), ("c", 3), ("b",7))
    )

    val rdd2: RDD[(String, Int)] = sc.makeRDD(
      List(("b", 4), ("c", 5), ("c", 6))
    )

    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    println(rdd3.collect().mkString(","))

  }

}
