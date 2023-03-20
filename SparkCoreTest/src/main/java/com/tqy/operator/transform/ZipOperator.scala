package com.tqy.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ZipOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6), 2
    )

    //必须分区数相同Can't zip RDDs with unequal numbers of partitions
//    val rdd2: RDD[Int] = sc.makeRDD(
//      List( 2, 3, 4, 5, 6), 1
//
//    )
    //Can only zip RDDs with same number of elements in each partition
//    val rdd2: RDD[Int] = sc.makeRDD(
//      List( 2, 3, 4, 5, 6), 2
//
//    )



    val rdd2: RDD[String] = sc.makeRDD(
      List( "2", "3", "4", "5", "6", "7"), 2

    )

//    val rdd2: RDD[Int] = sc.makeRDD(
//      List( 2, 3, 4, 5, 6, 7), 2
//
//    )

    val rdd4: RDD[(Int, String)] = rdd1.zip(rdd2)

    println(rdd4.collect().mkString(","))

  }

}
