package com.hjx.weblog.spark.common

import org.apache.spark.{Logging, SparkConf}

/**
  * Created by Axin in 2019/11/18 21:27
  *
  * SparkConf 工厂
  */
object SparkConfFactory extends  Serializable with Logging{


  /**
    * sparkCore 离线本地批量处理SparkConf
    * @param appName
    * @param threads
    * @return
    */
  def newSparkLoalConf( appName : String = "spark local" , threads : Int = 1 ) =
    new SparkConf().setMaster(s"local[$threads]").setAppName(appName)

  /**
    * sparkCore 离线集群批量处理SparkConf
    * @param appName
    * @return
    */
  def newSparkConf(appName:String = "default") : SparkConf = {
    new SparkConf().setAppName(appName)
  }



}
