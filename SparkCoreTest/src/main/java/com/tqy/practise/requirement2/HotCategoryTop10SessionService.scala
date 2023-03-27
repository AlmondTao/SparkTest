package com.tqy.practise.requirement2

import com.tqy.practise.requirement2.Programme1.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object HotCategoryTop10SessionService{


def main (args: Array[String] ): Unit = {

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

    val topIds: Array[String] = rdd5.take(10).map(_._1)


    val actionDatas = rdd1.map(
        data => {
            val datas = data.split("_")
            UserVisitAction(
                datas(0),
                datas(1).toLong,
                datas(2),
                datas(3).toLong,
                datas(4),
                datas(5),
                datas(6).toLong,
                datas(7).toLong,
                datas(8),
                datas(9),
                datas(10),
                datas(11),
                datas(12).toLong
            )
        }
    )

    val clickDatas = actionDatas.filter {
        data => {
            if ( data.click_category_id != -1 ) {
                topIds.contains(data.click_category_id.toString)
            } else {
                false
            }
        }
    }

    val reduceDatas = clickDatas.map(
        data => {
            (( data.click_category_id, data.session_id ), 1)
        }
    ).reduceByKey(_+_)

    val groupDatas: RDD[(Long, Iterable[(String, Int)])] = reduceDatas.map {
        case ((cid, sid), cnt) => {
            (cid, (sid, cnt))
        }
    }.groupByKey()

    val result: Array[(Long, List[(String, Int)])] = groupDatas.mapValues(
        iter => {
            iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
        }
    ).collect()

    result.foreach(println(_))

}





}


