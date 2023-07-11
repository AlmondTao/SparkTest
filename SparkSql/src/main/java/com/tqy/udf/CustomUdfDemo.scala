package com.tqy.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomUdfDemo{

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("udfDemo")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json("input2/User.json")

    session.udf.register("printUser",(name:String,age:Int) => s"name:${name},age:${age}")

    df.createTempView("User")

    val df2: DataFrame = session.sql("select printUser(name,age) from User")

    df2.show()

  }



}
