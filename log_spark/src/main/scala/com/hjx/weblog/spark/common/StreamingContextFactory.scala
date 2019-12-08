package com.hjx.weblog.spark.common

import org.apache.spark.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author: KING
  * description:
  * Date:Created in 2019-08-17 20:14
  */
object StreamingContextFactory extends Serializable with Logging{


  /**
    * 创建本地流streamingContext
    * @param appName             appName
    * @param batchInterval      多少秒读取一次
    * @param threads             开启多少个线程
    * @return
    */
  def newSparkLocalStreamingContext(appName:String = "sparkStreaming" ,
                                    batchInterval:Long = 30L ,
                                    threads : Int = 4) : StreamingContext = {

    val sparkConf =  SparkConfFactory.newSparkLoalConf(appName, threads)
    // sparkConf.set("spark.streaming.receiver.maxRate","10000")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","6")
    new StreamingContext(sparkConf, Seconds(batchInterval))
  }


  /**
    * 创建本地流streamingContext
    * @param appName             appName
    * @param batchInterval      多少秒读取一次
    * @return
    */
  def newSparkStreamingContext(appName:String = "sparkStreaming" ,
                                    batchInterval:Long = 30L) : StreamingContext = {

    val sparkConf =  SparkConfFactory.newSparkConf(appName)
    // sparkConf.set("spark.streaming.receiver.maxRate","10000")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
    new StreamingContext(sparkConf, Seconds(batchInterval))
  }




}
