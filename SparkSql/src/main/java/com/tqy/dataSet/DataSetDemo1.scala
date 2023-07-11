package com.tqy.dataSet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataSetDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
//    val sc = new SparkContext(conf)

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json("sparkSqlInput/User.json")

//    df.show()

    //sql风格语法
    df.createTempView("User")
//    session.sql("select avg(userage) from User").show()

    //DSL风格语法
//    df.select("username","userage").show()
    import session.implicits._
//    df.select($"username",$"userage"+1).show()
//    df.select('username,'userage+1).show()


    //*****RDD=>DataFrame=>DataSet*****
    //rdd
    val personRdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "zhangsan", 18), (2, "lisi", 19), (3, "wangwu", 20), (4, "zhaoliu", 21)))
    //Rdd => DataFrame
    val personDf: DataFrame = personRdd.toDF("id","name","age")
//    personDf.show()
    //DataFrame => DataSet
    val personDs: Dataset[Person] = personDf.as[Person]
//    personDs.show()


    //*****DataSet=>DataFrame=>RDD*****
    //DataSet => DataFrame
    val personDf2: DataFrame = personDs.toDF()
//    personDf2.show()
    //DataFrame => Rdd
    val personRdd2: RDD[Row] = personDf2.rdd
//    personRdd2.collect().foreach(r => println(r.get(0)))


    //Rdd => DataSet
    val personDs2: Dataset[Person] = personRdd.map {
      case (id, name, age) => new Person(id, name, age)
    }.toDS()
//    personDs2.show()

    //DataSet => Rdd
    val personRdd3: RDD[Person] = personDs2.rdd
    personRdd3.collect().foreach(println(_))

    //释放资源
    session.close()


  }

  case class Person(id:Int,name:String,age:Int)
}
