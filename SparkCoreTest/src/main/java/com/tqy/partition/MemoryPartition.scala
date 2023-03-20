package com.tqy.partition

import org.apache.spark.{SparkConf, SparkContext}

object MemoryPartition {
  def main(args: Array[String]): Unit = {

    //8
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    sparkConf.set("spark.default.parallelism","4")
    val sc:SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(
      List("1", "2", "3", "4", "5", "6"),2
    )

    //1.makeRDD方法numSlices默认值 defaultParallelism
    //2.1.1如果是local模式会获取spark.default.parallelism配置的数，若没配置则取totalCores
    //2.1.2totalCores与master相关：
    // ①local默认为1
    // ②local[N] 为N ，local[*]为Runtime.getRuntime.availableProcessors()
    // ③local[*, M] 与②类似，M为最大任务失败次数
    //2.2如果是standalone模式会取spark.default.parallelism， 若没配置则取 totalCoreCount和2 之间的最大值



    //结果为2
    rdd.saveAsTextFile("output")
  }

}
