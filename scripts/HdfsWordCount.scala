package com.streaming.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsWordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream("hdfs://localhost:9000/test.txt")
    
    
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXX")
    wordCounts.print()
    System.out.println("-----------------------")
    ssc.start()
    ssc.awaitTermination()
  }







}
// scalastyle:on println
