package com.tqy.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyOperator {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("c", 3),
        ("b", 4), ("c", 5), ("c", 6)
      ), 2
    )
    // TODO : 取出每个分区内相同key的最大值然后分区间相加
    // aggregateByKey算子是函数柯里化，存在两个参数列表
    // 1. 第一个参数列表中的参数表示初始值
    // 2. 第二个参数列表中含有两个参数
    //    2.1 第一个参数表示分区内的计算规则
    //    2.2 第二个参数表示分区间的计算规则

    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => {
        Math.max(x, y)
      },
      (x, y) => {
        x + y
      }
    )

    println(rdd1.collect().mkString(","))




  }

}
