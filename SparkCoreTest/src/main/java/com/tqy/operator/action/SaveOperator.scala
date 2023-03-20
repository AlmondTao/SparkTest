package com.tqy.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SaveOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd: RDD[Person] = sc.makeRDD(
      List(Person("zhangsan", 20), Person("lisi", 21), Person("wangwu", 22), Person("zhaoliu", 23)),2
    )

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    val rdd2: RDD[(String, Int)] = rdd.map(data =>(data.toString, 1))
    rdd2.saveAsSequenceFile("output2")

  }

  class Person(name:String,age:Int) extends Serializable {
    override def toString: String = s"name:${name},age:${age}"
  }

  object Person{

    def apply(name: String, age: Int): Person = new Person(name, age)
  }

}
