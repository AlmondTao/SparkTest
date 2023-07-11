package com.tqy.readFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("jsonDemo")

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json("sparkSqlInput/User.json")
    df.show()
    df.write.format("json").save("sparkSqlOutput/json/User.json")


  }

}
