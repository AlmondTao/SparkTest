package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FilterOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6),2
    )

    //当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜。
    val rdd2: RDD[Int] = rdd.filter(
      num => num != 2 && num !=3
    )

    rdd2.foreach(println(_))

    rdd.saveAsTextFile("output")
    rdd2.saveAsTextFile("output2")

  }
}
