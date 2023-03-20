package com.tqy.partition

import org.apache.spark.{SparkConf, SparkContext}

object FilePartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc:SparkContext = new SparkContext(sparkConf)





    //1.1 textFile方法使用的TextInputStream(FileInputStream) getSplits 方法
    // long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    // long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);
    // SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";
    //2.1如果文件是可切分的：
    //  long blockSize = file.getBlockSize();
    //  long splitSize = Math.max(minSize, Math.min(goalSize, blockSize));


    //1@@       0   1  2
    //2@@       3   4  5
    //3@@       6   7  8
    //4@@       9  10 11
    //5         12
    //goalSize = 13/2 = 6
    //minSize = 1
    //blockSize = 33554432 = 32M
    //splitSize = 6
    //13/6 = 2...1   1/6 > 1.1?
    //[0,6] =
    //[6,12] =
    //[12,13] =


    val rdd = sc.textFile("input/partition.txt",2)



    rdd.saveAsTextFile("output")
  }
}
