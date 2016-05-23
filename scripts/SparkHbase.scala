package com.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import com.esotericsoftware.kryo.Kryo
import org.apache.spark._
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue


/**
 * @author hadoop
 */


object SparkHbase {
  
 
  def main(args: Array[String]): Unit = {
   
    val conf = new SparkConf().setAppName("test1").setMaster("local[3]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf);
    
    
    @transient val hbaseconf = new Configuration();
    hbaseconf.set("hbase.master", "localhost:60010")
    hbaseconf.set(TableInputFormat.INPUT_TABLE, "urlkeyword")
    
    hbaseconf.set("hbase.defaults.for.version.skip", "true")


    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(hbaseconf)
   
    hbaseconf.set(TableInputFormat.SCAN_COLUMNS, "f:c");    

    val hBaseRDD = sc.newAPIHadoopRDD(hbaseconf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      
      val res = hBaseRDD.take(3)
        for (j <- 1 to 2) {
           val rs = res(j - 1)._2
           var kvs = rs.raw
            for (kv <- kvs) 
              println("row:" + new String(kv.getRow()) +
                " cf:" + new String(kv.getFamily()) +
                " column:" + new String(kv.getQualifier()) +
                " value:" + new String(kv.getValue()))
      
      val myTable = new HTable(hbaseconf, TableName.valueOf("urlkeyword"))
       myTable.setAutoFlush(false, false)
       myTable.setWriteBufferSize(3*1024*1024)
          val p = new Put(Bytes.toBytes("www.qq.com"))
          p.add("f".getBytes, "c".getBytes, Bytes.toBytes(1))
          myTable.put(p)
      myTable.flushCommits()
        }
    sc.stop()
    admin.close()
  }
  
  def KeyValueToString(keyValues: Array[KeyValue]): String = {
    val it = keyValues.iterator
    val res = new StringBuilder
    while (it.hasNext) {
      res.append( Bytes.toString(it.next.getValue()) + ",")
    }
    res.substring(0, res.length-1);
}
   
  
}