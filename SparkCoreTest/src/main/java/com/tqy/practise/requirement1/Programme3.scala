package com.tqy.practise.requirement1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object Programme3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ClassRank")

    val sc = new SparkContext(conf)

    val accumulator = new HotCategoryAccumulator()

    sc.register(accumulator)

    val rdd1: RDD[String] = sc.textFile("input/user_visit_action.txt")


    val rdd2: RDD[String] = rdd1.filter(data => {
      data.split("_")(5) == "null"
    })

    rdd2.foreach(
      data => {
        val dataArr: Array[String] = data.split("_")
        if (dataArr(6) != "-1") {

          accumulator.add((dataArr(6), "click"))
        } else if (dataArr(8) != "null") {
          dataArr(8).split(",").foreach(clazz => accumulator.add((clazz, "order")))

        } else {
          dataArr(10).split(",").foreach(clazz => accumulator.add((clazz, "pay")))

        }
      }
    )

    val order: Ordering[(Int, Int, Int)] = new Ordering[(Int, Int, Int)] {
      override def compare(x: (Int, Int, Int), y: (Int, Int, Int)): Int = {
        if (x._1 > y._1) {
          1
        } else if (x._1 == y._1) {
          if (x._2 > y._2) {
            1
          } else if (x._2 == y._2) {
            if (x._3 > y._3) {
              1
            }else{
              -1
            }
          }else{
            -1
          }

        }else{
          -1
        }



      }
    }

    val tuples: List[(String, (Int, Int, Int))] = accumulator.value.toList.sortBy(_._2)(order.reverse).take(10)

    tuples.foreach(println(_))


  }

  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,(Int,Int,Int)]]{

    val result : mutable.Map[String,(Int,Int,Int)] = new mutable.HashMap()

    override def isZero: Boolean = result.isEmpty

    override def copy(): AccumulatorV2[(String,String), mutable.Map[String, (Int, Int, Int)]] = new HotCategoryAccumulator()

    override def reset(): Unit = result.clear()

    override def add(v: (String,String)): Unit = {
      val (clickCount,orderCount,payCount) = result.getOrElse(v._1,(0,0,0))

      if (v._2 == "click"){
        result.update(v._1,(clickCount+1,orderCount,payCount))
      }else if(v._2 == "order"){
        result.update(v._1,(clickCount,orderCount+1,payCount))
      }else{
        result.update(v._1,(clickCount,orderCount,payCount+1))
      }


    }

    override def merge(other: AccumulatorV2[(String,String), mutable.Map[String, (Int, Int, Int)]]): Unit = {
      val otherResult: mutable.Map[String, (Int, Int, Int)] = other.value

      otherResult.foldLeft(result)(
        (thisData,otherData)=>{
          val (clickCount,orderCount,payCount) = thisData.getOrElse(otherData._1,(0,0,0))
          thisData.update(otherData._1,(clickCount+otherData._2._1,orderCount+otherData._2._2,payCount+otherData._2._3))
          thisData
        }
      )


    }

    override def value: mutable.Map[String, (Int, Int, Int)] = result
  }

}
