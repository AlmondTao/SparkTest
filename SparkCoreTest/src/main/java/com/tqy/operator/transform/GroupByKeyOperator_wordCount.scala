package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupByKeyOperator_wordCount {def main(args: Array[String]): Unit = {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
  val sc = new SparkContext(sparkConf)

  val rdd: RDD[String] = sc.textFile("input/wordcount.txt")

  val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

  val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

  val rdd3: RDD[(String, Iterable[Int])] = rdd2.groupByKey()

  val rdd4: RDD[(String, Int)] = rdd3.mapValues(_.size)


  println(rdd4.collect().mkString(","))



}
}
