package com.tqy.readFormat

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object MysqlWriteDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mysqlDemo")

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val rdd: RDD[User] = session.sparkContext.makeRDD(List(User("zhangsan2", 18), User("lisi2", 19), User("wangwu2", 20)))

    import session.implicits._

    val ds: Dataset[User] = rdd.toDS()

    ds.write.mode(SaveMode.Append).format("jdbc")
          .option("url", "jdbc:mysql://node01:3306/test")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "123456")
          .option("dbtable", "User").save()


  }

  case class User(username:String,userage:Int,id:Option[Int]=None)
}
