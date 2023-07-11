package com.tqy.practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

import scala.collection.{breakOut, mutable}
import scala.util.control.Breaks.{break, breakable}

object Practice1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("practice1")
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import session.implicits._

    session.udf.register("getCityPercent",functions.udaf(new GetCityPercent) )

    session.sql("use myhive;")


    session.sql("select count(1) from user_visit_action where click_product_id != '';").show()
    session.sql(
      s"""
         |select c.area,c.city_name,p.product_name
         |from
         |(select click_product_id,city_id from user_visit_action where click_product_id != '-1') t1
         |left join
         |product_info p
         |on t1.click_product_id = p.product_id
         |left join
         |city_info c
         |on t1.city_id = c.city_id
         |""".stripMargin).createTempView("tab1")

    session.sql(
      s"""
         |select area,product_name,count(1) click_count,getCityPercent(city_name,1) common
         |from
         |tab1
         |group by area,product_name
         |""".stripMargin).createTempView("tab2")

    session.sql(
      s"""
         |select
         |area,
         |product_name,
         |click_count,
         |rank() over(partition by area order by click_count desc) rankNum,
         |common
         |from
         |tab2
         |""".stripMargin).createTempView("tab3")

    session.sql(
      s"""
         |select area,product_name,click_count,common
         |from
         |tab3
         |where
         |rankNum <= 3
         |""".stripMargin).createTempView("tab4")


    session.sql(
      s"""
         |drop table if exists myhive.result
         |""".stripMargin)

    session.sql(
      s"""
         |create table myhive.result as select * from tab4
         |""".stripMargin)



  }

  case class CityPercentBuf(var totalCount:Int,cityRank:mutable.Map[String,Int])

  class GetCityPercent extends Aggregator[(String,Int),CityPercentBuf,String]{

    override def zero: CityPercentBuf =  CityPercentBuf(0,mutable.Map.empty)

    override def reduce(b: CityPercentBuf, a: (String, Int)): CityPercentBuf = {
      b.totalCount+=1
      b.cityRank(a._1) = b.cityRank.getOrElse(a._1,0)+a._2
      b
    }

    override def merge(b1: CityPercentBuf, b2: CityPercentBuf): CityPercentBuf = {
      b1.totalCount+=b2.totalCount
      b2.cityRank.foreach(t=>{
        b1.cityRank(t._1) = b1.cityRank.getOrElse(t._1,0)+t._2
      })
      b1
    }

    override def finish(reduction: CityPercentBuf): String = {
      val cityList: List[(String, Int)] = reduction.cityRank.toList.sortBy(_._2)(Ordering.Int.reverse)
      val totalCount: Int = reduction.totalCount
      val builder = new StringBuilder

      //方法一
//      var top2count = 0
//      breakable{
//        for (i <- 0 to cityList.size-1){
//          val tuple: (String, Int) = cityList(i)
//
//          if (builder.isEmpty){
//            builder.append(s"${tuple._1} ${(tuple._2*1000/totalCount).toDouble/10}%")
//          } else if (i>1){
//            builder.append(s"，其他 ${((totalCount-top2count)*1000/totalCount).toDouble/10}%")
//            break()
//          } else {
//            builder.append(s"，${tuple._1} ${(tuple._2*1000/totalCount).toDouble/10}%")
//          }
//          top2count+=tuple._2
//        }
//      }
      //方法二
      val top2Str: String = cityList.take(2).map {
        case (cityName, clickCount) => cityName + " " + ( clickCount * 1000 / totalCount).toDouble / 10 +"%"
      }.mkString("，")
      builder.append(top2Str)
      if (cityList.size>2){
        val top2sum: Int = cityList.map(_._2).take(2).sum
        builder.append(s"，其他 ${((totalCount-top2sum)*1000/totalCount).toDouble/10}%")
      }
      builder.toString()
    }

    override def bufferEncoder: Encoder[CityPercentBuf] = Encoders.product[CityPercentBuf]

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }




}
