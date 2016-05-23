package com.streaming.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds


/**
 * @author hadoop
 */
object SaleAmount {
def main(args: Array[String]) {
  if (args.length != 2) {
     System.err.println("Usage: SaleAmount <hostname> <port> ")
     System.exit(1)
  }
  

  
   val conf = new SparkConf().setAppName("SaleAmount").setMaster("local[2]")
   val sc = new SparkContext(conf)
   
   val sqlContext=new org.apache.spark.sql.SQLContext(sc)
   val ssc = new StreamingContext(conf, Seconds(5))
   
   import sqlContext.implicits._
   
// 通过 Socket 获取数据,该处需要提供 Socket 的主机名和端口号,数据保存在内存和硬盘中
  val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
  val words = lines.map(_.split(",")).filter(_.length == 6)
  val wordCounts = words.map(x=>(1, x(5).toDouble)).reduceByKey(_ + _)
  wordCounts.print()
  ssc.start() 
  ssc.awaitTermination()
} }
