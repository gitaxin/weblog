package com.hjx.weblog.spark.common

import com.hjx.weblog.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager

/**
  *
  */
object KafkaDSUtil {




  def getLocalKafkaDS(topicStr:String,
                      appName:String,
                      ssc:StreamingContext,
                      sutoUpdateoffset:Boolean): DStream[java.util.Map[String,String]] = {
    //定义topic
    val topics = topicStr.split(",")
    //首先从kafka读取数据
   // val ssc = StreamingContextFactory.newSparkLocalStreamingContext(appName, batchInterval, threads)
    //val ssc = StreamingContextFactory.newSparkStreamingContext("SparkstreamingKafka2esTest",10)
    //构建kafka参数
    val kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topicStr,appName)
    val kafkaManager = new KafkaManager(kafkaParams,sutoUpdateoffset)
    val mapDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc,Set(topicStr))
    mapDS
  }

  def getKafkaDS(topicStr:String,
                 appName:String,
                 batchInterval:Long,
                 sutoUpdateoffset:Boolean): DStream[java.util.Map[String,String]] = {
    //定义topic
    val topics = topicStr.split(",")
    //首先从kafka读取数据
    //val ssc = StreamingContextFactory.newSparkLocalStreamingContext(appName, batchInterval, threads)
    val ssc = StreamingContextFactory.newSparkStreamingContext(appName,batchInterval)
    //构建kafka参数
    val kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topicStr,appName)
    val kafkaManager = new KafkaManager(kafkaParams,sutoUpdateoffset)
    val mapDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc,Set(topicStr))
    mapDS
  }


}
