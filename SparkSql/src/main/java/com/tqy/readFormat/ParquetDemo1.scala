package com.tqy.readFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ParquetDemo1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("parquetDemo")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    //生成parquet文件
//    val df: DataFrame = session.read.format("json").load("sparkSqlInput/Student.json")
//    import session.implicits._
//    val ds: Dataset[Student] = df.as[Student]
    //    ds.show()
//    ds.write.format("parquet").save("sparkSqlOutput/parquetOutput")

    //读取parquet文件
    val df2: DataFrame = session.read.format("parquet").load("sparkSqlOutput/parquetOutput/part-00000-1a701e61-e085-4b0f-bb70-6b5c73ff6154-c000.snappy.parquet")
    df2.show()



  }


  case class Student(studentName:String,studentAge:BigInt,teacher: Teacher,courseList:List[Course]){

  }

  case class Teacher(teacherName:String,teacherAge:BigInt){

  }

  case class Course(courseName:String,courseScore:BigInt)
}
