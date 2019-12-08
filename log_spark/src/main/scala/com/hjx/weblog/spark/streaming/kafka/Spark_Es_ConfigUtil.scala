package com.hjx.weblog.spark.streaming.kafka

import org.apache.spark.Logging

/**
  * author:
  * description:
  * Date:Created in 2019-04-21 22:20
  */
object Spark_Es_ConfigUtil extends Serializable with Logging{

  val ES_NODES = "es.nodes"
  val ES_PORT = "es.port"
  val ES_CLUSTERNAME = "es.clustername"


  /**
    * es.mapping.id 参数说明：
    *指定了ID，ES会使用数据中指定的字段作为ID，再写入包含相同ID的数据的时候，则进行更新操作
    * 如果不指定ID，ES会自动生成ID，这样的话，再存入数据的时候，会生成新的ID，则会存入重复数据
    *
    */
  //
  //
  def getEsParam(id_field : String): Map[String,String] ={
    Map[String ,String]("es.mapping.id" -> id_field, //以Map数据哪个字段作为ES 的ID
      ES_NODES -> "bigdata003",
      ES_PORT -> "9200",
      ES_CLUSTERNAME -> "my-application",
      "es.batch.size.entries"->"6000",
      /*   "es.nodes.wan.only"->"true",*/
      "es.nodes.discovery"->"true",
      "es.batch.size.bytes"->"300000000",
      "es.batch.write.refresh"->"false"
    )
  }
}
