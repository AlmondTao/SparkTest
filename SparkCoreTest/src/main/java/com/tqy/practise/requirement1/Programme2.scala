package com.tqy.practise.requirement1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Programme2 {
  def main(args: Array[String]): Unit = {
    //品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量来统计热门品类。
    //本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ClassRank")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val rdd2: RDD[String] = rdd1.filter(data => {
      data.split("_")(5) == "null"
    })

    val rdd3: RDD[(String, (Int, Int, Int))] = rdd2.flatMap(data => {
      val dataArr: Array[String] = data.split("_")
      if (dataArr(6) != "-1") {
        Array((dataArr(6), (1, 0, 0)))
      } else if (dataArr(8) != "null") {
        dataArr(8).split(",").map(clazz => (clazz, (0, 1, 0)))
      } else {
        dataArr(10).split(",").map(clazz => (clazz, (0, 0, 1)))
      }
    })

    val rdd4: RDD[(String, (Int, Int, Int))] = rdd3.reduceByKey((x, y) => {
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    })

    val rdd5: RDD[(String, (Int, Int, Int))] = rdd4.sortBy(_._2, false)

    rdd5.take(10).foreach(println(_))


  }

}
