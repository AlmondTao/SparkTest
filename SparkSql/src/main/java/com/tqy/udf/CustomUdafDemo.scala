package com.tqy.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator


object CustomUdafDemo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UdafDemo")

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json("input2/User.json")

    df.createTempView("User")

    val udaf: MyAveragUDAF = new MyAveragUDAF()
    session.udf.register("MyAverage",functions.udaf(udaf))

    val df2: DataFrame = session.sql("select MyAverage(age) from User")

    df2.show()

  }

  class MyAveragUDAF extends Aggregator[Long,MyBuf,Double]{

    val myBuf:MyBuf = MyBuf(0,0)

    override def zero: MyBuf = MyBuf(0,0)

    override def reduce(b: MyBuf, a: Long): MyBuf = {
      b.totalCount +=1
      b.totalAge += a
      b
    }

    override def merge(b1: MyBuf, b2: MyBuf): MyBuf = {
      b1.totalAge += b2.totalAge
      b1.totalCount += b2.totalCount
      b1
    }

    override def finish(reduction: MyBuf): Double = {
      reduction.totalAge.toDouble/reduction.totalCount
    }

    override def bufferEncoder: Encoder[MyBuf] = Encoders.product[MyBuf]

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }




  case class MyBuf(var totalCount:Long,var totalAge:Long)

}
