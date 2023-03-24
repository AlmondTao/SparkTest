package com.tqy.persist

import org.apache.spark.{SparkConf, SparkContext}

object CheckPointTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("cp")

    val lines = sc.makeRDD(
      List("Hadoop Hbase Hbase", "Spark Scala Spark")
    )
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      t => {
        println("*************************")
        (t, 1)
      }
    )
    // Spark可以将中间的计算结果保存到检查点中，让其他的应用使用数据
    // Checkpoint directory has not been set in the SparkContext
    // 检查点可以切断血缘关系。
    // 检查点为了数据的安全，会重新再执行一遍作业，所以会执行2次
    // 为了解决这个问题，可以将检查点和缓存联合使用
    wordToOne.checkpoint()
    wordToOne.cache()
    val wordToCount = wordToOne.reduceByKey(
      (x,y) =>{
        x+y
      })
    println(wordToCount.toDebugString)
    println(wordToOne.collect().mkString(","))//.foreach(println)
    println("--------------------------------------------")
    // val rdd2: RDD[(Int, Iterable[(String, Int)])] = wordToOne.groupBy(_._2)
    // rdd2.collect()
    println(wordToCount.toDebugString)
    println(wordToCount.count())
  }
}
