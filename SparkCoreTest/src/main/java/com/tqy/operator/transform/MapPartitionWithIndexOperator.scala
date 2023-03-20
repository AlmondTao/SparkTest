package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapPartitionWithIndexOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5),3
    )

    val rdd2: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((index, _))
      }
    )

    rdd2.collect().foreach(println(_))



  }

}
