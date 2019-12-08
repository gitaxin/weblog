package com.hjx.weblog.spark.streaming.hbase



import java.util

import com.hjx.weblog.hbase.config.HBaseTableUtil
import com.hjx.weblog.hbase.insert.HBaseInsertHelper
import com.hjx.weblog.hbase.split.SpiltRegionUtil
import com.hjx.weblog.spark.common.{KafkaDSUtil, StreamingContextFactory}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging

import scala.collection.JavaConversions._

/**
  * Created by Axin in 2019/11/30 23:02
  *
  * 使用Spark 整合 Hbase
  *
  *
  * Exception:异常
  * java.lang.SecurityException: class "javax.servlet.ServletRegistration"'s signer information does not match signer information of other classes in the same package
  * 为包冲突：解决方法，在pom文件中排除冲突的依赖
  */
object Kafka2hbase extends Serializable with Logging{


  def main(args: Array[String]): Unit = {


    val tableName = "test:chl_test0"
    HBaseTableUtil.createTable(tableName,"cf",true,-1,100,true,SpiltRegionUtil.getSplitKeysBydinct)
    // topicStr:String,
    // appName:String,
    // batchInterval:Long,
    //threads:Int,
    //sutoUpdateoffset:Boolean
    val topicStr = "chl_test0"
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext("Kafka2hbase", 10L, 1)
    val kafkaDS = KafkaDSUtil.getLocalKafkaDS(topicStr,
      "Kafka2hbase",
      ssc,
      false)

    kafkaDS.foreachRDD(rdd=>{
      rdd.take(2).foreach(println(_))

      val putRDD = rdd.map(x=>{
        val rowKey = x.get("id").toString

        val put = new Put(rowKey.getBytes)
        //获取x中的所有key
        val keys = x.keySet()
        keys.foreach(key=>{
          put.addColumn("cf".getBytes(),Bytes.toBytes(key),Bytes.toBytes(x.get(key)))
        })
        put
      })
      putRDD.foreachPartition(partion=>{
        //将partion  PUT 转为list[PUT]
        val list = new util.ArrayList[Put]()
        while (partion.hasNext){
          list.add(partion.next())
        }
        HBaseInsertHelper.put(tableName,list)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
