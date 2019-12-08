package com.hjx.weblog.spark.streaming.kafka.es


import com.hjx.weblog.spark.streaming.kafka.Spark_Es_ConfigUtil
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.rdd.EsSpark


object Kafka2EsJob extends Serializable with Logging{



  /**
    * 按类型拆分写入
    * @param dataType
    * @param typeDS
    */
  def insertData2Es(dataType:String,typeDS:DStream[java.util.Map[String,String]]): Unit ={
    val index = dataType
    typeDS.foreachRDD(rdd=>{
      EsSpark.saveToEs(rdd,index+ "/"+index,Spark_Es_ConfigUtil.getEsParam("id"))
      println("写入ES" + rdd.count() + "条数据成功")
    })
  }
}
