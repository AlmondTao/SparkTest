package com.tqy.operator.transform.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test1 {

  def main(args: Array[String]): Unit = {
    //TODO
    //1)	数据准备
    //agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    //2)	需求描述
    //统计出每一个省份每个广告被点击数量排行的Top3

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("input/agent.txt")

    val rdd2: RDD[((String, String), Int)] = rdd1.map(lines => {
      val strArr: Array[String] = lines.split(" ")
      ((strArr(1), strArr(4)), 1)
    })

    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey(_ + _)

    val rdd4: RDD[(String, (String, Int))] = rdd3.map(t => (t._1._1, (t._1._2, t._2)))


    val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey()

    val rdd6: RDD[(String, List[(String, Int)])] = rdd5.mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))


    rdd6.foreach(println(_))
  }

}
