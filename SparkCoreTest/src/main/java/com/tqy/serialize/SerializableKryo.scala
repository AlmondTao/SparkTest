package com.tqy.serialize

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SerializableKryo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Search]))

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(
      Array("hello word","hello spark")
    )

    val search = new Search("hello")
    //error
        println(search.getMatch1(rdd).collect().mkString(","))
    //error
        println(search.getMatch2(rdd).collect().mkString(","))

//    println(search.getMatch3(rdd).collect().mkString(","))



  }

  class Search(query:String) extends Serializable {

    def isMatch(s:String): Boolean ={
      s.contains(query)
    }

    def getMatch1(rdd:RDD[String]) ={

      rdd.filter(isMatch)

    }

    def getMatch2(rdd:RDD[String]) ={
      rdd.filter(_.contains(query))

    }

    def getMatch3(rdd:RDD[String]) ={


      val s = query
      rdd.filter(_.contains(s))

    }

  }
}
