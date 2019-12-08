package org.apache.spark.streaming.kafka

import com.hjx.weblog.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}

/**
  * Created by Axin in 2019/11/17 11:34
  *
  *
  * scala 2.10.4下载地址：
  * https://www.scala-lang.org/download/2.10.4.html
  */
object KafkaClusterTest {



  val topic = "chl_test0"
  //val groupId = "111"
  val groupId = "console-consumer-83687"


  /**
    * 包名要创建为spark包下，否则KafkaCluster是导入不进来的，
    * 在kafkaCluster的类上有 private[spark] 修饰：表示只能在spark包下进行访问
    */
  var kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topic,groupId)

  //@transient仿止序列化
  @transient
  private var cluster = new KafkaCluster(kafkaParams)


  def main(args: Array[String]): Unit = {

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

    println("打印分区信息:"+partitions)
    partitions.foreach(println(_))





    //当执行创建topic命令时，在zookeeper中就已经存在offset了

    //TODO 获取最早偏移
    val earliestLeaderOffsetsE:Either[Err, Map[TopicAndPartition, LeaderOffset]] = cluster.getEarliestLeaderOffsets(partitions)
    //断言，如果是right = true ，取出值

    require(earliestLeaderOffsetsE.isRight,s"获取earliestLeaderOffsetsE失败")

    val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
    println("打印最早偏移信息")
    earliestLeaderOffsets.foreach(println(_))


    //TODO 获取最末偏移
    val latestLeaderOffsetsE:Either[Err, Map[TopicAndPartition, LeaderOffset]] = cluster.getLatestLeaderOffsets(partitions)
    //断言，如果是right = true ，取出值

    require(latestLeaderOffsetsE.isRight,s"latestLeaderOffsets失败")

    val latestLeaderOffsets = latestLeaderOffsetsE.right.get
    println("打印最末偏移信息")
    latestLeaderOffsets.foreach(println(_))


  //TODO 获取消费者组的offsets
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


  }




}
