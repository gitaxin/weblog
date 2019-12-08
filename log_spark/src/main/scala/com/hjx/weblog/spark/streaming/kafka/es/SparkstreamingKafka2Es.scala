package com.hjx.weblog.spark.streaming.kafka.es

import java.util

import com.hjx.weblog.admin.AdminUtil
import com.hjx.weblog.client.ESclientUtil
import com.hjx.weblog.common.properties.DataTypeProperties
import com.hjx.weblog.common.time.TimeTranstationUtils
import com.hjx.weblog.spark.common.StreamingContextFactory
import com.hjx.weblog.spark.common.convert.DataConvert
import com.hjx.weblog.spark.streaming.kafka.{Spark_Es_ConfigUtil, Spark_Kafka_ConfigUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka.KafkaManager
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.JavaConversions._

/**
  * Created by Axin in 2019/11/18 23:03
  *
  *
  * Shift+Ctrl+Alt+N 查找字段依赖
  */
object SparkstreamingKafka2Es extends  Serializable with Logging{


  private val topic = "chl_test0"
  //val groupId = "111"
  private val groupId = "111"




  //val array = Array("wechat","qq","mail")

  private val tableSet: util.Set[String] = DataTypeProperties.dataTypeMap.keySet()

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("com.bh.d406.bigdata").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val topics = args(1).split(",")
    //首先从kafka读取数据
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext("SparkstreamingKafka2Es"
      ,5L,2)
    var kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topic,groupId)
    val kafkaManager = new KafkaManager(kafkaParams)

    val mapDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc,Set(topic))
        //数据加工处理，添加日期字段
        .map(map=>{
        //TODO 值应该是时间的日期形式，通过时间戳转化
        map.put("dayPartion",TimeTranstationUtils.Date2yyyyMMdd(java.lang.Long.valueOf(map.get("collect_time")+"000")))
        map
      })

    val client = ESclientUtil.getClient
    //mapDS里面包含三种数据类型
    //需要对mapDS按类别进行过滤
    //import scala.collection.JavaConversions._ 将Java类转换为scala类   tableSet=>tableSet
    tableSet.foreach(table =>{
      //qq
      val tableDS = mapDS.filter(line =>{table.equals(line.get("table"))})


      //按类型拆分
      //Kafka2EsJob.insertData2Es(table,tableDS)
      tableDS.foreachRDD(rdd =>{
        //继续按天数拆分
        //索引名
        //拿到所有天数

        val arrayDays = rdd.map(x=>{x.get("dayPartion")}).distinct()
          .collect() //把所有数据拉到driver上

        arrayDays.foreach(x=>println("打印日期"+ x))

        arrayDays.foreach(day =>{
          val table_dayRDD = rdd.filter(line => {day.equals(line.get("dayPartion"))})
            .map(line=>{
              //将MAP[string,String]转为MAP[string,Object]
              DataConvert.strMap2esObjectMap(line)
            })
          //索引名 = 类型 + 日期
          val index = table +"_" + day


          //首先判断索引是不是存在
          //不存在的时候才创建
          val existIndex = AdminUtil.indexExists(client,index)
          if(!existIndex){
            val path = s"test/es/mapping/${table}.json"
            AdminUtil.buildIndexAndTypes(index, index, path, 5, 1)
          }

          //6.x版本ES,一个index只能有一个type(相当于一个库只能有一个表),所以我们将index和type设为相同
          EsSpark.saveToEs(table_dayRDD, s"${index}/${index}",Spark_Es_ConfigUtil.getEsParam("id"))

        })


      })

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
