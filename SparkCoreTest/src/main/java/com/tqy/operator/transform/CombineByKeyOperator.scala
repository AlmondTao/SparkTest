package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CombineByKeyOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("c", 3),
        ("b", 4), ("c", 5), ("c", 6)
      ), 2
    )

    //TODO 将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))求每个key的平均值


    val rdd2: RDD[(String, (Int, Int))] = rdd1.combineByKey((_, 1),
      (x: Tuple2[Int, Int], y) => {
        (x._1 + y, x._2 + 1)
      },
      (x: (Int, Int), y: (Int, Int)) => {
        (x._1 + y._1, x._2 + y._2)
      }
    )

    val rdd3: RDD[(String, Double)] = rdd2.mapValues(datas => (datas._1.toDouble / datas._2.toDouble))

    println(rdd3.collect().mkString(","))

  }

}
