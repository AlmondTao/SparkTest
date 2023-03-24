package com.tqy.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

object HashPartitionerTest {

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

//    val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new HashPartitioner(4))
    //默认分区器就是hashPartitioner
    val rdd2: RDD[(Int, Iterable[String])] = rdd1.groupByKey()
    val rdd3: RDD[(Int, Iterable[String])] = rdd1.groupByKey(new HashPartitioner(4))


    rdd2.foreachPartition(data =>{
      println(data.mkString(","))
    })
    println("-----------------------")
    rdd3.foreachPartition(data =>{
      println(data.mkString(","))
    })

    println("-----------------------")



//    println(rdd2.collect().mkString(","))


  }
}
