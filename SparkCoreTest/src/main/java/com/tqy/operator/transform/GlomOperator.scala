package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GlomOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6),2
    )

    val rdd2: RDD[Int] = rdd.glom().map(_.sum)

    rdd2.foreach(println(_))

  }
}
