package com.tqy

import org.apache.spark.launcher._

object CMDTest {


  def main(args: Array[String]): Unit = {
    var cmd:String = "java -version "
    val str: String = MyCommandBuilderUtils.quoteForBatchScript(cmd)
    System.out.println(str)

  }


}
