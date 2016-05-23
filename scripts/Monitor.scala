package com.streaming.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.Logging

import org.apache.log4j.{Level, Logger}

object Monitor extends Logging{
  def main(args: Array[String]) {
    
   val sparkConf = new SparkConf().setAppName("FileWordCount")
// 创建 Streaming 的上下文,包括 Spark 的配置和时间间隔,这里时间为间隔 20 秒
   val ssc = new StreamingContext(sparkConf, Seconds(20))
// 指定监控的目录,在这里为/home/hadoop/temp/
   val lines = ssc.textFileStream("file:///home/hadoop/temp")
   
// 对指定文件夹变化的数据进行单词统计并且打印
   val words = lines.flatMap(_.split(" "))
   val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
   System.out.println("--------------------------------")
   wordCounts.print()
   System.out.print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
   ssc.start()
   ssc.awaitTermination()

  }
}
// scalastyle:on println
