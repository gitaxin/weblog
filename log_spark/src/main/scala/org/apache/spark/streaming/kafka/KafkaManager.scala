package org.apache.spark.streaming.kafka

import com.alibaba.fastjson.TypeReference
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err

import scala.reflect.ClassTag

/**
  * Created by Axin in 2019/11/17 16:56  
  */
class KafkaManager (val kafkaParams: Map[String,String],
                    val autoUpdateOffset : Boolean = true) extends Serializable with Logging {


  @transient
  private var cluster = new KafkaCluster(kafkaParams)


  //单例模式
  def kc():KafkaCluster = {
    if(cluster == null){
      cluster = new KafkaCluster(kafkaParams)
    }
      cluster
  }



  /**
    * 创建记录偏移的DS
    * @param ssc
    * @param topicsSet
    * @param converter
    * @tparam T
    * @return
    */
  def createDirectStreamWithOffset[T:ClassTag](ssc:StreamingContext ,
                                               topicsSet:Set[String],
                                               converter:String => T): DStream[T] = {


    createDirectStream[String, String, StringDecoder, StringDecoder](ssc, topicsSet)
      //(null,{"rksj":"1566067141","latitude":"24.000000","imsi":"000000000000000","accept_message":"","phone_mac":"aa-aa-aa-aa-aa-aa","device_mac
      .map(pair =>converter(pair._2))
  }





  //封装一下 去除不需要的内容
  /**
    * 读取JSON的流 并将JSON流 转为MAP流  并且这个流支持RDD向zookeeper中记录消费信息
    * @param ssc   spark ssc
    * @param topicsSet topic 集合 支持从多个kafka topic同时读取数据
    * @return  DStream[java.util.Map[String,String
    */
  def createJsonToJMapStringDirectStreamWithOffset(ssc:StreamingContext ,
                                                   topicsSet:Set[String]): DStream[java.util.Map[String,String]] = {

    // converter 方法   接收一个json字符串
    // 方法体就是把一个json字符串 转成一个MAP
    val converter = {
      json:String =>{

        var res : java.util.Map[String,String] = null
        try {
          res = com.alibaba.fastjson.JSON.parseObject(json, new TypeReference[java.util.Map[String, String]]() {})
        } catch {
          case e: Exception => logError(s"解析topic ${topicsSet}, 的记录 ${json} 失败。", e)
        }
        res
      }
    }
    createDirectStreamWithOffset(ssc, topicsSet, converter).filter(_ != null)
  }







  /*def createDirectStream[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag,
  R: ClassTag] (
                 ssc: StreamingContext,
                 kafkaParams: Map[String, String],
                 fromOffsets: Map[TopicAndPartition, Long],
                 messageHandler: MessageAndMetadata[K, V] => R
               ): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    new DirectKafkaInputDStream[K, V, KD, VD, R](
      ssc, kafkaParams, fromOffsets, cleanedHandler)
  }*/
  //对原始方法进行封装,从消费者组的offset 开始读取数据
  def createDirectStream[
      K: ClassTag,
      V: ClassTag,
      KD <: Decoder[K]: ClassTag,
      VD <: Decoder[V]: ClassTag] (
                                    ssc: StreamingContext,
                                    topics: Set[String]
                                  ): InputDStream[(K, V)] = {


    val groupId = kafkaParams.get("group.id").getOrElse("default")

    //一个新的消费者组消费topic时，无偏移量会报错，所以加个判断。判断是否存在偏移量，不存在则更新
    setOrUpdateOffsets(topics,groupId)


    val messages = {

      //获取topic chl_test0 的分区，返回一个Either类型
      val partitionE: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topics)

      require(partitionE.isRight, s"获取partions失败")
      val partitions: Set[TopicAndPartition] = partitionE.right.get

      //获取消费者组的偏移
      val consumerOffsetsE = cluster.getConsumerOffsets(groupId, partitions)

      require(consumerOffsetsE.isRight, s"获取consumerOffsets失败")
      val consumerOffsets = consumerOffsetsE.right.get



      //从上一次消费者组消费的地方开始消费数据
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
        ssc,
        kafkaParams,
        consumerOffsets,
        (mmd: MessageAndMetadata[K, V]) => (mmd.key(), mmd.message())
      )
    }



    //消费完成之后，需要把消费者组的偏移进行更新（把新的消费者偏移写入到zookeeper中）

    //更新消费者偏移
    //给一个默认级别
    if (autoUpdateOffset) {
      messages.foreachRDD(rdd => {
        logInfo("RDD消费成功，开始更新zk上的偏移量")
        updateZKOffsets(rdd)
      })
    }
    //返回messages
    messages
  }


  /**
    * 把消费者组的offsets更新到zk中
    * @param rdd
    */
  def updateZKOffsets[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) : Unit = {

    //获取消费者组
    val groupId = kafkaParams.get("group.id").getOrElse("default")
    //spark使用kafka低阶API进行消费的时候,每个partion的offset是保存在 spark的RDD中，所以这里可以直接在
    //RDD的 HasOffsetRanges 中获取倒offsets信息。因为这个信息spark不会把则个信息存储到zookeeper中，所以
    // 在zookeeper上读取offsets前先根据实际情况更新offsets
    //我们需要自己实现将这部分offsets信息存储到zookeeper中
    //拿到RDD中的offsets
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //打印出spark中保存的offsets信息
    offsetsList.foreach(x=>{
      println("获取spark 中的偏移信息"+x)
    })

    for (offsets <- offsetsList) {
      //根据topic和partition 构建topicAndPartition
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      logInfo("将SPARK中的 偏移信息 存到zookeeper中")
      //将消费者组的offsets更新到zookeeper中
      setOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
    }
  }

  /**
    * 更新偏移
    * @param groupId
    * @param offsets
    */
  private def setOffsets(groupId: String, offsets: Map[TopicAndPartition, Long]): Unit ={
    if(offsets.nonEmpty){
      //通过KafkaCluster   setConsumerOffsets 更新Consumer的Offsets
      //这一步就是更新Consumer的Offsets 到ZK中
      val o = kc().setConsumerOffsets(groupId, offsets)
      logInfo(s"更新zookeeper中消费组为：${groupId} 的 topic offset信息为： ${offsets}")
      if (o.isLeft) {
        logError(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }



  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    *
    * @param topics
    * @param groupId
    */
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {

      //获取kafka  partions的节点信息
      val partitionsE = kc.getPartitions(Set(topic))
      logInfo(partitionsE+"")
      //检测
      require(partitionsE.isRight, s"获取 kafka topic ${topic}`s partition 失败。")
      val partitions = partitionsE.right.get

      //获取最早的 partions offsets信息
      val earliestLeader = kc.getEarliestLeaderOffsets(partitions)
      val earliestLeaderOffsets = earliestLeader.right.get
      println("kafka中最早的消息偏移")
      earliestLeaderOffsets.foreach(println(_))


      //获取最末的 partions offsets信息
      val latestLeader = kc.getLatestLeaderOffsets(partitions)
      val latestLeaderOffsets = latestLeader.right.get
      println("kafka中最末的消息偏移")
      latestLeaderOffsets.foreach(println(_))

      //获取消费者组的 offsets信息
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      //如果消费者offset存在
      if (consumerOffsetsE.isRight) {
        /**
          * 如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        //如果earliestLeader 存在
        if(earliestLeader.isRight) {
          //获取最早的offset 也就是最小的offset
          val earliestLeaderOffsets = earliestLeader.right.get
          //获取消费者组的offset
          val consumerOffsets = consumerOffsetsE.right.get
          // 将 consumerOffsets 和 earliestLeaderOffsets 的offsets 做比较
          // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
          var offsets: Map[TopicAndPartition, Long] = Map()

          consumerOffsets.foreach({ case (tp, n) =>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            //如果消費者的偏移小于 kafka中最早的offset,那麽，將最早的offset更新到zk
            if (n < earliestLeaderOffset) {
              logWarning("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
                " offsets已经过时，更新为" + earliestLeaderOffset)
              offsets += (tp -> earliestLeaderOffset)
            }
          })
          //设置offsets
          setOffsets(groupId, offsets)
        }
      } else {
        // 消费者还没有消费过  也就是zookeeper中还没有消费者的信息
        if(earliestLeader.isLeft)
          logError(s"${topic} hasConsumed but earliestLeaderOffsets is null。")
        //看是从头消费还是从末开始消费  smallest表示从头开始消费
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase).getOrElse("smallest")
        //构建消费者 偏移
        var leaderOffsets: Map[TopicAndPartition, Long] = Map.empty
        //从头消费
        if (reset.equals("smallest")) {
          //分为 存在 和 不存在 最早的消费记录 两种情况
          //如果kafka 最小偏移存在，则将消费者偏移设置为和kafka偏移一样
          if(earliestLeader.isRight){
            leaderOffsets = earliestLeader.right.get.map {
              case (tp, offset) => (tp, offset.offset)
            }
          }else{
            // 如果不存在，则从新构建偏移全部为0 offsets
            leaderOffsets = partitions.map(tp => (tp, 0L)).toMap
          }
        } else {
          //直接获取最新的offset
          leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get.map {
            case (tp, offset) => (tp, offset.offset)
          }
        }
        //设置offsets
        setOffsets(groupId, leaderOffsets)
      }
    })
  }



}
