package com.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author hadoop
 */
object UrlPVTop10 {
  
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("test1").setMaster("local[3]")
      val sc = new SparkContext(conf);
      
      val file = sc.textFile("hdfs://localhost:9000/log.spark")
      // file.flatMap(_.split(",")).map(x => (x, 1)).reduceByKey(_ + _)
      val words = file.map(_.split("\t")).map(f => (f(1), 1))
      .reduceByKey(_ + _).map(x => (x._2,x._1)).sortByKey(true).top(10)
      
      // 
      //reduce
      
      
      //13121212  www.baidu.com  92  2012
      //13121212,www.baidu.com,92,2012
      //13121212,www.baidu.com,92,2012
      //13121212,www.baidu.com,92,2012
      //13121212,www.baidu.com,92,2012
      
      
      //--- RDD ~ [13121212,www.baidu.com  92  2012]

      
  }
}