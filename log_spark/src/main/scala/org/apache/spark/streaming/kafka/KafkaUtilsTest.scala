package org.apache.spark.streaming.kafka

import com.hjx.weblog.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by Axin in 2019/11/17 15:18
  *
  * KafkaUtils   Demo
  * KafkaUtils 是spark用来获取stream的工具类
  *
  * <!--spark1.6需对应scala2.10-->
  *
  *
  * 直接消费
  */
object KafkaUtilsTest extends Serializable with Logging{

  val topic = "chl_test0"
  //val groupId = "111"
  val groupId = "console-consumer-83687"


  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf
    sparkConf.setAppName("KafkaUtilsTest").setMaster("local[1]")

    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext,Seconds(10L))

    val kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topic,groupId)



    /**
      * def createDirectStream[
      * K: ClassTag,
      * V: ClassTag,
      * KD <: Decoder[K]: ClassTag,
      * VD <: Decoder[V]: ClassTag] (
      * ssc: StreamingContext,
      * kafkaParams: Map[String, String],
      * topics: Set[String]
      * ): InputDStream[(K, V)] = {
      * val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
      * val kc = new KafkaCluster(kafkaParams)
      * val fromOffsets = getFromOffsets(kc, kafkaParams, topics)
      * new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
      * ssc, kafkaParams, fromOffsets, messageHandler)
      * }
      */

    val DS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))

    DS.foreachRDD(rdd =>{
      val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //打印RDD中消费者组的偏移
      offsetList.foreach(x=>println(s"获取RDD中的偏移信息${x}"))
      //rdd中保存的offset，每次程序重启后，都是从0开始读取的，所以这个offset要进行保存到zookeeper或redis

      rdd.foreach(println(_))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
