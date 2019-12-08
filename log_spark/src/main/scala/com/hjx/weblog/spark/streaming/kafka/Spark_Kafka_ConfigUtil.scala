package com.hjx.weblog.spark.streaming.kafka

/**
  * 获取Kafka参数
  * Created by Axin in 2019/11/17 11:51  
  */
object Spark_Kafka_ConfigUtil {


  /**

    * metadata.broker.list=bigdata002:9092
    * zk.connect = bigdata001:2181,bigdata002:2181,bigdata003:2181
    * serializer.class=kafka.serializer.StringEncoder
    * #request.required.acks=1
    * request.timeout.ms=60000
    * producer.type=sync
    *
    * @return
    */

  def getKafkaParam(kafkaTopic:String,groupId : String):Map[String,String] ={


    /**
      * auto.offset.reset：
      *   smallest表示从最小偏移量消费
      *   largest表示从最后偏移量消费
      *   如果发生异常后，还是无法保证最准确的偏移量，所以要自己实现
      *   例如：10-20，读到15时发生的异常，重启后要么从10开始读，要么从20开始读，不能从15开始读
      */
    val kafkaParam=Map[String,String](
      "metadata.broker.list" -> "bigdata002:9092",
      "auto.offset.reset" -> "smallest", //表示从最小偏移量消费
      "group.id" -> groupId,
      "refresh.leader.backoff.ms" -> "1000",
      "num.consumer.fetchers" -> "8")
    kafkaParam
  }

}
