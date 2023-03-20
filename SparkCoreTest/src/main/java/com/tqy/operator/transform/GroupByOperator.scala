package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupByOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6),2
    )

    val rdd2: RDD[(Int, Iterable[Int])] = rdd.groupBy((_ % 3),2)

    rdd2.foreach(println(_))
    //一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
    rdd2.saveAsTextFile("output")
  }


}
