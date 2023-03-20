package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CoalesceOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),2
    )
    rdd.foreachPartition(
      datas=>println(datas.mkString(","))
    )

    val rdd1: RDD[Int] = rdd.coalesce(4,true)
    println("***********")
    rdd1.saveAsTextFile("output")



  }
}
