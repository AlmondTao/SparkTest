package com.tqy.partitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

object MyPartitionerTest {

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

    implicit var ctor :Ordering[Int] = new Ordering[Int]{
      override def compare(x: Int, y: Int): Int = x-y
    }
    val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new MyPartitioner(3))


    rdd2.foreachPartition(
      data =>{
      println(data.mkString(","))
    }
    )




  }
  class MyPartitioner[k:Ordering:ClassTag,v](numPartitions2:Int) extends Partitioner {


    override def numPartitions: Int = {numPartitions2}

    override def getPartition(key: Any): Int = {
      if (!key.isInstanceOf[Int]){
        0
      }else{
        val i: Int = key.asInstanceOf[Int]
        i%numPartitions
      }
    }


  }


}
