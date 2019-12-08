package com.hjx.weblog.spark.common

import org.apache.spark.{Logging, SparkContext}

/**
  * Created by Axin in 2019/11/27 20:58  
  */
object SparkContextFactory extends  Serializable with Logging{


  def newSparkLocalBatchContext(appName : String = "spark local" , threads : Int = 1 ): SparkContext ={
    val sparkConf = SparkConfFactory.newSparkLoalConf(appName,threads)
    new SparkContext(sparkConf)
  }
}
