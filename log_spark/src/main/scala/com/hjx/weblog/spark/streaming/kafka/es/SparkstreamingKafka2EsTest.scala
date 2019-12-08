package com.hjx.weblog.spark.streaming.kafka.es

import com.hjx.weblog.spark.common.StreamingContextFactory
import com.hjx.weblog.spark.streaming.kafka.{Spark_Es_ConfigUtil, Spark_Kafka_ConfigUtil}
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.KafkaManager
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by Axin in 2019/11/18 21:22
  *
  * 整合 elasticsearch测试
  */
object SparkstreamingKafka2EsTest extends  Serializable with Logging{



  private val topic = "chl_test0"
  //val groupId = "111"
  private val groupId = "111"

  private var kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topic,groupId)

  def main(args: Array[String]): Unit = {

    //首先从kafka读取数据

    val ssc = StreamingContextFactory.newSparkLocalStreamingContext("SparkstreamingKafka2EsTest"
          ,5L,2)

    val kafkaManager = new KafkaManager(kafkaParams)

    val mapDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc,Set(topic))
    //val DS = kafkaManager.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,Set(topic))

    mapDS.foreachRDD(rdd =>{

      rdd.take(2).foreach(println(_))
      //走的HTTP协议     9200
      //tranclient走的tcp协议  9300
      //会自动创建索引和type
      //resource 代表ES中的  index/type
      EsSpark.saveToEs(rdd, "index/index",Spark_Es_ConfigUtil.getEsParam("id"))
      println(s"写入ES${rdd.count()}条数据" )

    })

    //使用EsSpark往ES写入数据
   // EsSpark.savetoes


//    DS.foreachRDD(rdd =>{
//      rdd.foreach(println(_))
//
//      val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      //打印RDD中消费者组的偏移
//      offsetList.foreach(x=>println(s"获取RDD中的偏移信息${x}"))
//
//
//    })

    ssc.start()
    ssc.awaitTermination()

    //写入ES



  }

}
