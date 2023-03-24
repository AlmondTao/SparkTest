package com.tqy.partitioner

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RangePartitionerTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Part")

    val sc = new SparkContext(conf)


    val rdd1: RDD[(Int, String)] = sc.makeRDD(
      List(
        (1, "台球"),
        (1, "高尔夫"),
        (2, "羽毛球"),
        (2, "乒乓球"),
        (11, "足球"),
        (15, "橄榄球"),
        (5, "篮球"),
        (1,"举重"),
        (8,"排球"),
        (4,"冰壶"),
        (4,"羽毛球双打")
      ),4
    )

    val rdd4: RDD[(Int, Iterable[String])] = rdd1.groupByKey(new RangePartitioner(4,rdd1))

    rdd4.foreachPartition(data =>{
      println(data.mkString(","))
    })

  }
}
