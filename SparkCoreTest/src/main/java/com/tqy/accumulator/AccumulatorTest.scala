package com.tqy.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc:SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5)
    ,2
    )

    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    //结果为0，因为sum被分别发送到各个executor中进行了累加，但没有传回给driver
    var sum:Int = 0
    rdd1.foreach(sum += _)
    println(sum)
    println("-----------------------")

    rdd1.foreach(sumAcc.add(_))
    println(sumAcc.value)













  }

}
