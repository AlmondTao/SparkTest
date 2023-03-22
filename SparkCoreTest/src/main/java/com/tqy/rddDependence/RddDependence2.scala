package com.tqy.rddDependence

import org.apache.spark.{SparkConf, SparkContext}

object RddDependence2 {
  def main(args: Array[String]): Unit = {


      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
      val context:SparkContext = new SparkContext(sparkConf)

      val fileRdd = context.textFile("input/wordcount.txt")

      val flatList = fileRdd.flatMap(_.split(" "))
      val groupMap = flatList.groupBy(word => word)
      val result = groupMap.mapValues(_.size)


    println(fileRdd.dependencies)
    println("-------------------------------------------------------------------------------")
    println(flatList.dependencies)
    println("-------------------------------------------------------------------------------")
    println(groupMap.dependencies)
    println("-------------------------------------------------------------------------------")
    println(result.dependencies)
    println("-------------------------------------------------------------------------------")


    println(result.collect().mkString(","))
//
//    result.saveAsTextFile("out")
//    context.stop()

}
}
