package com.tqy.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceOperator {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )
    //若不指定分区则每次运行结果不同
    val reduceResult: Int = rdd.reduce(_ - _)


    println(reduceResult)


  }

}
