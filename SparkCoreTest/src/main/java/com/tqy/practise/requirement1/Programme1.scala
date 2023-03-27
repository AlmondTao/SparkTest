package com.tqy.practise.requirement1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io

object Programme1 {

  def main(args: Array[String]): Unit = {
    //品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量来统计热门品类。
    //本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ClassRank")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val rdd2: RDD[Array[String]] = rdd1.map(data => {
      val dataArr: Array[String] = data.split("_")
      dataArr
    })

    val rdd3: RDD[Array[String]] = rdd2.filter(
      dataArr => {
        if (dataArr(5) == "null")
          true
        else
          false
      }
    )


    val rdd4: RDD[(String, Int)] = rdd3.flatMap(
      dataArr => {
        if (dataArr(6) != "-1") {
          Array((dataArr(6) + "-click", 1))
        } else if (dataArr(8) != "null") {
          val classArr: Array[String] = dataArr(8).split(",")
          classArr.map(clazz => (clazz + "-order", 1))
        } else {
          val classArr: Array[String] = dataArr(10).split(",")
          classArr.map(clazz => (clazz + "-pay", 1))
        }
      }
    )

//    rdd4.foreach(data =>println(_))

    val rdd5: RDD[(String, Int)] = rdd4.reduceByKey(_ + _)

    val rdd6: RDD[(String, (String, Int))] = rdd5.map(datas => {
      val dataArr: Array[String] = datas._1.split("-")
      (dataArr(0), (dataArr(1), datas._2))
    })

    val rdd7: RDD[(String, Iterable[(String, Int)])] = rdd6.groupByKey()

    val rdd8: RDD[(String, (Int, Int, Int))] = rdd7.mapValues(datas => {
      var clickCount: Int = 0
      var orderCount: Int = 0
      var payCount: Int = 0
      datas.foreach(
        typeAndCount => {
          if (typeAndCount._1 == "click")
            clickCount = typeAndCount._2
          else if (typeAndCount._1 == "order")
            orderCount = typeAndCount._2
          else if (typeAndCount._1 == "pay")
            payCount = typeAndCount._2
        }
      )
      (clickCount, orderCount, payCount)
    })

    val result: Array[(String, (Int, Int, Int))] = rdd8.sortBy(_._2, false).take(10)


    result.foreach(println(_))


//    val rdd9: RDD[(String, (Int, Int, Int))] = rdd8.sortBy(_._2, false)
//需要先collect再foreach ，否则顺序不对
//    rdd9.collect().foreach(data => println(data))

  }

}
