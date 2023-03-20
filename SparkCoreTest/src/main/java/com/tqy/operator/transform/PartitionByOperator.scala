package com.tqy.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionByOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 1), ("b", 2), ("c", 3) ,("d", 4),("e",5)), 2
    )

    val rdd1: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(3))

    rdd.saveAsTextFile("output")
    rdd1.saveAsTextFile("output2")

  }
}
