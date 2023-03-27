package com.tqy.practise.requirement2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Programme1 {
  def main(args: Array[String]): Unit = {
    //在需求一的基础上，增加每个品类用户session的点击统计


    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ClassRank")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val rdd2: RDD[String] = rdd1.filter(data => {
      data.split("_")(5) == "null"
    })

    rdd2.cache()

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

    val top10category: Array[String] = rdd5.take(10).map(_._1)


    val nRdd3: RDD[UserVisitAction] = rdd2.map(data => {
      val dataArr: Array[String] = data.split("_")
      new UserVisitAction(
        dataArr(0),
        dataArr(1).toLong,
        dataArr(2),
        dataArr(3).toLong,
        dataArr(4),
        dataArr(5),
        dataArr(6).toLong,
        dataArr(7).toLong,
        dataArr(8),
        dataArr(9),
        dataArr(10),
        dataArr(11),
        dataArr(12).toLong
      )
    })
    val nRdd4: RDD[UserVisitAction] = nRdd3.filter(u =>u.click_category_id.toString != "-1" && top10category.contains(u.click_category_id.toString))

    val nRdd5: RDD[((Long, String), Int)] = nRdd4.map(u => ((u.click_category_id, u.session_id), 1))

    val nRdd6: RDD[((Long, String), Int)] = nRdd5.reduceByKey(_ + _)

    val nRdd7: RDD[(Long, (String, Int))] = nRdd6.map {
      case ((categoryId, sessionId), count) => (categoryId, (sessionId, count))
    }

    val nRdd8: RDD[(Long, Iterable[(String, Int)])] = nRdd7.groupByKey()

    val nRdd9: RDD[(Long, List[(String, Int)])] = nRdd8.mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(10))

    nRdd9.collect().foreach(println(_))

  }

  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,//用户的ID
    session_id: String,//Session的ID
    page_id: Long,//某个页面的ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Long,//某一个商品品类的ID
    click_product_id: Long,//某一个商品的ID
    order_category_ids: String,//一次订单中所有品类的ID集合
    order_product_ids: String,//一次订单中所有商品的ID集合
    pay_category_ids: String,//一次支付中所有品类的ID集合
    pay_product_ids: String,//一次支付中所有商品的ID集合
    city_id: Long //城市 id
                            )



}
