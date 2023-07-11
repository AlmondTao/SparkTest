package com.tqy.readFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HiveDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]").setAppName("hivesql")
      //在开发工具中创建数据库默认是在本地仓库，通过参数修改数据库仓库的地址
      .set("spark.sql.warehouse.dir","hdfs://node01:8020/tqy")
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val df: DataFrame = session.read.json("sparkSqlInput/User.json")
    df.show()

    val showtables: DataFrame = session.sql("show tables;")

    showtables.show()

    val showdatabases: DataFrame = session.sql("show databases;")
    showdatabases.show()

    val useMyhive: DataFrame = session.sql("use myhive;")
    useMyhive.show()
    import session.implicits._
    val ds: Dataset[User] = df.as[User]

    ds.createTempView("User")

//    val df2: DataFrame = session.sql("drop table User_info")
//    df2.show()

    val df3: DataFrame = session.sql("create table User_info select * from User")

    df3.show()

    session.sql("insert into User_info  select id,username,userage from User")

  }


  case class User(id:Long,username:String,userage:Long)

}
