package com.tqy.partition

import org.apache.spark.{SparkConf, SparkContext}

object FilePartition2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc:SparkContext = new SparkContext(sparkConf)
    //1.1 textFile方法使用的TextInputStream(FileInputStream) getSplits 方法
    // long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    // long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);
    // SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";
    //2.1如果文件是可切分的：
    //  long blockSize = file.getBlockSize();32M
    //  long splitSize = Math.max(minSize, Math.min(goalSize, blockSize));


    //123       123@@       0  1  2  3  4
    //45        45@@        5  6  7  8
    //678       678@@       9  10 11 12 13
    //9         9           14
    //goalSize = 15/3 = 5
    //minSize = 1
    //blockSize = 33554432 = 32M
    //splitSize = 5
    //15/5 = 3....0      (5/5 > 1.1)?
    //[0,偏移量5] = 123 45
    //[5,偏移量5] = 678
    //[10,偏移量5] = 9
    val rdd = sc.textFile("input/partition2.txt",3)
    rdd.saveAsTextFile("output")
  }
}
