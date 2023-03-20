package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FoldByKeyOperator_wordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("input/wordcount.txt")

    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))

    val rdd3: RDD[(String, Int)] = rdd2.map((_, 1))

    val rdd4: RDD[(String, Int)] = rdd3.foldByKey(0)(_ + _)

    println(rdd4.collect().mkString(","))

  }

}
