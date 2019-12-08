package org.apache.spark.streaming.kafka

import com.hjx.weblog.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
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
  * 根据消费者组偏移进行消费，但是不更新到zk中
  */
object KafkaUtilsTest1 extends Serializable with Logging{



  val topic = "chl_test0"
  //val groupId = "111"
  val groupId = "console-consumer-83687"

  var kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topic,groupId)

  //@transient仿止序列化
  @transient
  private var cluster = new KafkaCluster(kafkaParams)



  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf
    sparkConf.setAppName("KafkaUtilsTest").setMaster("local[1]")

    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext,Seconds(10L))


    //TODO 获取消费者组的offsets

    //获取topic chl_test0 的分区，返回一个Either类型
    val partitionE:Either[Err,Set[TopicAndPartition]] = cluster.getPartitions(Set(topic))

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
    val consumerOffsetsE = cluster.getConsumerOffsets(groupId,partitions)

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

    val DS = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
      ssc,
      kafkaParams,
      consumerOffsets,
      (mmd:MessageAndMetadata[String,String]) => (mmd.key(),mmd.message())
    )

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
