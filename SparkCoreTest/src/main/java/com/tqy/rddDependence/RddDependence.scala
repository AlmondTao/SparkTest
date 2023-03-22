package com.tqy.rddDependence

import org.apache.spark.{SparkConf, SparkContext}

object RddDependence {
  def main(args: Array[String]): Unit = {


      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
      val context:SparkContext = new SparkContext(sparkConf)

      val fileRdd = context.textFile("input/wordcount.txt")

      val flatList = fileRdd.flatMap(_.split(" "))
      val groupMap = flatList.groupBy(word => word)
      val result = groupMap.mapValues(_.size)


    println(fileRdd.toDebugString)
    println("-------------------------------------------------------------------------------")
    println(flatList.toDebugString)
    println("-------------------------------------------------------------------------------")
    println(groupMap.toDebugString)
    println("-------------------------------------------------------------------------------")
    println(result.toDebugString)
    println("-------------------------------------------------------------------------------")


    println(result.collect().mkString(","))
//
//    result.saveAsTextFile("out")
//    context.stop()

}
}
