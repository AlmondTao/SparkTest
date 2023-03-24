package com.tqy.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PersistTest {

  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Persist")
//
//    val sc = new SparkContext(conf)
//
//    val rdd: RDD[String] = sc.makeRDD(
//      List("a", "b", "c", "d", "e", "f"), 2
//    )
//    var count:Int = 0
//    val rdd1: RDD[(String, Int)] = rdd.map(data =>{
//      count = count+1
//      (data, count)
//    })
//
//    val rdd2: RDD[(String, Int)] = rdd1.sortBy(_._2, false)
//    //    rdd2.cache()
//    //    rdd2.persist(StorageLevel.DISK_ONLY)
//    println(rdd2.toDebugString)
//    println(rdd2.collect().mkString(","))

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

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
    // 设定数据持久化
    // cache方法可以将血缘关系进行修改，添加一个和缓存相关的依赖关系
    // 在执行了行动算子后会增加血缘关系
    // cache操作不安全。
    wordToOne.cache()
    // 如果持久化的话，那么持久化的文件只能自己用。而且使用完毕后， 会删除
//    wordToOne.persist(StorageLevel.DISK_ONLY_2)

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

    sc.stop()

  }

}
