package com.tqy.practise.requirement3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Programme1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ClassRank")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("input/user_visit_action.txt")

    // 【1，2，3，4，5，6，7】
    val okIds = List("1","2","3","4","5","6","7")
    // 【（1，2），（2，3）】
    val okFlowIds = okIds.zip(okIds.tail)

    val rdd2: RDD[(String, (String, String))] = rdd1.map(
      data => {
        val dataArr: Array[String] = data.split("_")
        (dataArr(2), (dataArr(4),dataArr(3)))
      }
    )
    val rdd3: RDD[(String, Iterable[(String, String)])] = rdd2.groupByKey()



    val rdd4: RDD[(String, Iterator[(String, String)])] = rdd3.mapValues(
      data => {
        var pageList: List[String] = data.toList.sortBy(_._1).map(_._2)
        pageList = pageList :+ "null"

        val iterator: Iterator[List[String]] = pageList.sliding(2, 1)
        iterator.map(idata => {
            if (idata.size > 1){
              if (idata(0) == idata(1)){
                println("++++++++++++++++++++++++++++")
              }
              (idata(0), idata(1))
            }else {
              (idata(0),"null")
            }


          })
//        val iterator: Iterator[List[String]] = data.toList.sortBy(_._1).map(_._2).sliding(2, 1)
//        iterator.map(data =>)


      }
    )
//    val filterRdd4: RDD[(String, Iterator[(String, String)])] = rdd4.filter(_._2.isEmpty)


    val rdd5: RDD[(String, String)] = rdd4.flatMap(_._2)

    val rdd6: RDD[(String, Iterable[String])] = rdd5.groupByKey()

//    rdd6.filter(data =>okIds.contains(data._1)).collect().foreach(println(_))

    val rdd7: RDD[((String, String), Double)] = rdd6.flatMap(
      data => {
        val nPageList: Iterable[String] = data._2
        val fPage = nPageList.size
        val tupleToDouble: Map[(String, String), Double] = nPageList.groupBy(np => np).map(npData => ((data._1, npData._1), npData._2.size.toDouble / fPage.toDouble))
        tupleToDouble

      }
    )


//    rdd7.collect().foreach(println(_))
    rdd7.filter(data =>okFlowIds.contains(data._1)).collect().foreach(println(_))

  }

}
