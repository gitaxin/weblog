package org.apache.spark.streaming.kafka

import com.hjx.weblog.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaCluster.Err
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
  * 根据消费者组偏移进行消费，并且更新消费者组偏移到ZK中
  */
object KafkaUtilsTest2 extends Serializable with Logging{

  val topic = "chl_test0"
  //val groupId = "111"
  val groupId = "111"


  private var kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topic,groupId)

  //@transient仿止序列化
  @transient
  private var cluster = new KafkaCluster(kafkaParams)



  def main(args: Array[String]): Unit = {


    /**
      * 实验：
      *  sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
      * 每秒钟每个partition最多消费1条，
      * 共有3个partition,3个partition相当于每秒钟只能消费3条，
      * Seconds(5L)
      * 5秒钟消费15条
      * 3*1*5 = 15
      */

    val sparkConf = new SparkConf
    sparkConf.setAppName("KafkaUtilsTest").setMaster("local[1]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","30")

    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext,Seconds(5L))


    //TODO 获取消费者组的offsets

    //获取topic chl_test0 的分区，返回一个Either类型
    val partitionE:Either[Err,Set[TopicAndPartition]] = cluster.getPartitions(Set("chl_test0"))

    //Either类型判断 chl_test0 是否有内容
    val left = partitionE.isLeft //left :正确信息 true: false:存在
    val right = partitionE.isRight //right :错误信息 true: false:存在

    println(left)
    println(right)


    //如果断言partitionE.isRight为true，则继续执行，否则拋异常,中断执行
    // val partitionE = cluster.getPartitions(Set("chl_test4"))，因为没有，则会拋弃异常
    require(partitionE.isRight,s"获取partions失败")
    val partitions:Set[TopicAndPartition] = partitionE.right.get

    /**
      * 参数
      * groupId : scala.Predef.String,
      * topicAndPartitions : scala.Predef.Set[kafka.common.TopicAndPartition]
      *
      *
      *  zookeeper中查看消费者组
      *  ls /consumers
      */
    val consumerOffsetsE = cluster.getConsumerOffsets("console-consumer-83687",partitions)

    require(consumerOffsetsE.isRight,s"获取consumerOffsets失败")
    val consumerOffsets = consumerOffsetsE.right.get
    println("打印消费者组信息")
    consumerOffsets.foreach(println(_))



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


    val kafkaManager = new KafkaManager(kafkaParams)
    val DS = kafkaManager.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,Set(topic))

   /* val DS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
      ssc,
      kafkaParams,
      consumerOffsets,
      (mmd:MessageAndMetadata[String,String]) => (mmd.key(),mmd.message())
    )*/

    DS.foreachRDD(rdd =>{
     rdd.foreach(println(_))


        //http://localhost:4040/streaming/   #查看消费情况
        //http://localhost:4040/executors/   #查看内存等资源情况

      //模拟数据消费延迟的情境。
      /*rdd.foreachPartition(partition =>{
        while(true){

        }
      })*/


    })

    ssc.start()
    ssc.awaitTermination()
  }

}
