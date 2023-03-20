package com.tqy.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5)
    )

    val rdd2:RDD[Int] = rdd.map(_ * 2)

    val rdd3:RDD[String] = rdd2.map(num => num + "")

    rdd3.collect().foreach(println(_))


  }

}
