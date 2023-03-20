package com.tqy.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(
      List("a", "b", "c", "d","e")
      ,2
    )

    val result: (String, Int) = rdd.aggregate(new Tuple2[String, Int]("", 0))(
      (x, y) => {
        (x._1 + y, x._2 + 1)
      },
      (x, y) => {

        println("执行分区间merge")
        (x._1 + "-" + y._1, x._2 - y._2)
      }
    )



    println(result)

  }
}
