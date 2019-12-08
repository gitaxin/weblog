package com.hjx.weblog.spark.streaming.kafka.warn

import java.util.Timer

import com.hjx.weblog.common.time.TimeTranstationUtils
import com.hjx.weblog.redis.client.JedisUtil
import com.hjx.weblog.spark.common.StreamingContextFactory
import com.hjx.weblog.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import com.hjx.weblog.spark.warn.dao.WarningMessageDao
import com.hjx.weblog.spark.warn.domain.WarningMessage
import com.hjx.weblog.spark.warn.timer.{PhoneWarn, SyncRule2Redis, WechatWarn}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.KafkaManager
import redis.clients.jedis.Jedis

/**
  * author: KING
  * description: 告警任务
  * Date:Created in 2019-08-21 21:03
  */
object WarningStreamingTask extends Serializable with Logging {


  def main(args: Array[String]): Unit = {

    //TODO 定义一个定时器
    val timer: Timer = new Timer
    timer.schedule(new SyncRule2Redis, 0, 1 * 60 * 1000)

    //定义topic
    val topics = "chl_test0"
    //首先从kafka读取数据
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext("WarningStreamingTask", 5L, 2)
    //val ssc = StreamingContextFactory.newSparkStreamingContext("SparkstreamingKafka2esTest",10)
    //构建kafka参数
    val kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topics, "WarningStreamingTask")
    val kafkaManager = new KafkaManager(kafkaParams)
    val mapDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc, Set(topics))
      //数据加工处理，添加日期字段
      .map(map => {
      //TODO 值应该是时间的日期形式，通过时间戳转化
      map.put("dayPartion", TimeTranstationUtils.Date2yyyyMMdd(java.lang.Long.valueOf(map.get("collect_time") + "000")))
      map
    })

    //设定告警字段
    val array = Array("phone")
    println("》》》》》》》》》》》》》WarningStreamingTask")
    //mapDS.foreach(println(_))
    mapDS.foreachRDD(rdd => {

      println("》》》》》》》》》》》》》foreachRDD rdd count: " + rdd.count())
      rdd.foreachPartition(partion => {

        println("》》》》》》》》》》》》》foreachPartition partion count:" )
        //获取redis客户端
        var jedis15: Jedis = null
        var jedis14: Jedis = null
        try {
          jedis14 = JedisUtil.getJedis(14)
          jedis15 = JedisUtil.getJedis(15)
         // println("》》》》》》》》》》》》》partion：" + partion.mkString )
          var i = 0
          while (partion.hasNext) {

            println("》》》》》》》》》》》》》partion while index: " +  i)
            i += 1
            val lineMap = partion.next()
            println(lineMap)
            array.foreach(field => {
              //从kafka数据中获取所有预警字段的值  然后和redis中进行比对
              if (lineMap.containsKey(field)) {
                //获取比对字段值
                val fieldValue = lineMap.get(field)
                // 拼接REDIS KEY
                val redisKey = field + ":" + fieldValue
                //判断是不是比对上了
                val boolean = jedis15.exists(redisKey)
                if (boolean) {
                  //TODO  实现告警
                  //时间间隔控制,用于处理频繁告警
                  val warn_time = jedis15.hget(redisKey, "warn_time")
                  if (StringUtils.isNotBlank(warn_time)) {//时间间隔 大于30秒
                    if (System.currentTimeMillis() / 1000 - java.lang.Long.valueOf(warn_time) > 30 * 1000) {
                      //预警
                      println("告警")
                      warn(redisKey, jedis15, jedis14, lineMap)
                      jedis15.hset(redisKey, "warn_time", System.currentTimeMillis() / 1000 + "")
                    }
                  } else {
                    //预警
                    warn(redisKey, jedis15, jedis14, lineMap)
                    //刷新时间
                    jedis15.hset(redisKey, "warn_time", System.currentTimeMillis() / 1000 + "")

                  }

                } else {
                  println("未命中，不做任何操作")
                }
              }
            })
          }
        } catch {
          case e =>
        } finally {
          JedisUtil.close(jedis14)
          JedisUtil.close(jedis15)
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()


    def warn(redisKey: String, jedis15: Jedis, jedis14: Jedis, lineMap: java.util.Map[String, String]): Unit = {

      //println(s"命中REDIS的key${redisKey}，开始发送预警消息")
      //具体的告警任务
      //首先需要获取告警的内容
      //构造告警内容的时候，需要把 这条告警数据发送到哪里去，以什么形式发送需要带进去
      // 首先将REDIS中的告警内容获取出来
      val split = redisKey.split(":")
      if (split.length == 2) {
        //说明key是对的，开始封装消息体
        val message = new WarningMessage()

        val warn = split(0)

        //获取redis中的信息
        val redisMap = jedis15.hgetAll(redisKey)
        //规则ID
        message.setAlarmRuleid(redisMap.get("id"))
        message.setSendMobile(redisMap.get("send_mobile"))
        message.setSendType(redisMap.get("send_type"))
        message.setAccountid(redisMap.get("publisher"))
        message.setAlarmType("2")
        var warn_type = ""
        //封装告警内容
        if (message.getAlarmType.equals("2")) {
          warn_type = "【嫌疑人预警】=>"
        }
        //自由组装
        //获取嫌疑人地址
        //需要关联设备表  设备ID  设备的经纬度 设备的具体地址
        val device_number = lineMap.get("device_number")
        val address = jedis14.get(device_number)

        val latitude = lineMap.get("latitude")
        val longitude = lineMap.get("longitude")
        val collect_time = lineMap.get("collect_time")
        val warn_content = s"${warn_type}【手机号为:${redisMap.get("warn_fieldvalue")}】的人在" +
          s"${collect_time}出现在经纬度【${latitude},${longitude}】 具体地址为 ${address}"
        message.setSenfInfo(warn_content)
        //将告警的消息持久化到MYSQL中，方便界面异步刷新
        WarningMessageDao.insertWarningMessageReturnId(message)
        //写数据库之前 直接发送告警
        //判断市什么类型的告警
        //封装告警内容
        if (message.getAlarmType.equals("2")) {
          //嫌疑人告警
          val phoneWarn = new PhoneWarn()
          phoneWarn.warn(message)
        }
        if (message.getAlarmType.equals("1")) {
          //嫌疑人告警
          val wechatWarn = new WechatWarn()
          wechatWarn.warn(message)
        }
      }
    }


  }
}
