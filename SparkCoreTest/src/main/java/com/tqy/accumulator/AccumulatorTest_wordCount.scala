package com.tqy.accumulator

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccumulatorTest_wordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc:SparkContext = new SparkContext(conf)

    val wordCountAcc = new WordCountAccumulator()

    sc.register(wordCountAcc)

    val rdd = sc.textFile("input/wordcount.txt")



        val flatList = rdd.flatMap(_.split(" "))
        flatList.foreach(wordCountAcc.add(_))

    println(wordCountAcc.value.mkString(","))
  }

  class WordCountAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{

    val result:mutable.Map[String,Int] = mutable.Map()

    override def isZero: Boolean = result.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new WordCountAccumulator

    override def reset(): Unit = result.clear()

    override def add(v: String): Unit = {
      //(xxx) =  调用map的update方法
      result(v) = result.getOrElse(v,0) + 1
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {

      other.value.foldLeft(result)(
        (x,y)=>{
          x(y._1) = x.getOrElse(y._1,0)+ y._2
          x
        }
      )

    }


    override def value: mutable.Map[String, Int] = result
  }

}
