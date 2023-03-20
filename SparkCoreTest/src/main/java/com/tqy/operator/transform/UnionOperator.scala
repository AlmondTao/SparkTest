package com.tqy.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UnionOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6), 2
    )

    val rdd2: RDD[Int] = sc.makeRDD(
      List( 2, 3, 4, 5, 6), 1

    )
    //必须是相同类型
    val rdd3: RDD[String] = sc.makeRDD(
      List( "2", "3", "4", "5", "6"), 1

    )
    val rdd4: RDD[Int] = rdd1.union(rdd2)

    println(rdd4.collect().mkString(","))

  }

}
