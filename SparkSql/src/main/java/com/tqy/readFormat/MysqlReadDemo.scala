package com.tqy.readFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object MysqlReadDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mysqlDemo")

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //方法一
//    val df: DataFrame = session.read.format("jdbc")
//      .option("url", "jdbc:mysql://node01:3306/test")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "User")
//      .load()

    //方法二
//    val props = new Properties()
//    props.setProperty("user", "root")
//    props.setProperty("password", "123456")
//    val df: DataFrame = session.read.jdbc("jdbc:mysql://node01:3306/test?user=root&password=123456", "User", props)

    //方法三
    val df: DataFrame = session.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://node01:3306/test",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "password" -> "123456",
        "dbtable" -> "User")).load()


    df.show()

  }

}
