package com.hjx.weblog.spark.streaming.hbase


import com.hjx.weblog.common.config.ConfigUtil
import com.hjx.weblog.hbase.config.HBaseTableUtil
import com.hjx.weblog.hbase.insert.HBaseInsertHelper
import com.hjx.weblog.hbase.split.SpiltRegionUtil
import com.hjx.weblog.spark.common.{KafkaDSUtil, StreamingContextFactory}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging

/**
  * author: KING
  * description:  hbase 关联任务
  * Date:Created in 2019-09-01 20:05
  */
object DataRelationStreaming extends Serializable with Logging{
  //读取关联字段——表 配置
  val relationPath = "test/spark.hbase/relation.properties"
  //需要关联的字段
 val relationFields =  ConfigUtil.getInstance().getProperties(relationPath).get("relationfield").toString.split(",")
  def main(args: Array[String]): Unit = {

    //clearRelationTable()
    initRelationTables
    val topicStr = "chl_test0"
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext("DataRelationStreaming", 10L, 1)
    val kafkaDS = KafkaDSUtil.getLocalKafkaDS(topicStr,
      "DataRelationStreaming",
      ssc,
      false)

    //处理数据
    kafkaDS.foreachRDD(rdd=>{
      rdd.foreachPartition(partion => {
          //因为关联需要一条一条处理，不能用批
        while (partion.hasNext){
          val map = partion.next()
          //首先构建主关联表，写入之前需要判断是不是有关联字段，有才处理
          relationFields.foreach(relationField=>{
              //判断map中是不是包含这个key
             if(map.containsKey(relationField)){
               //说明有关联字段，需要进行关联
               //TODO 构建主关联表
               val phone_mac = map.get("phone_mac")
               val put = new Put(phone_mac.getBytes())
               //添加值(自定义版本号)
               //因为版本号直能为正整数，而我们这个值是字符串 所以要做一个  字符串=》整数 的映射
               val fieldValue = map.get(relationField)
               val versionNum = (relationField + fieldValue).hashCode & Integer.MAX_VALUE
               put.addColumn("cf".getBytes(),Bytes.toBytes(relationField),versionNum,Bytes.toBytes(fieldValue))
               HBaseInsertHelper.put("test:relation",put)


               //TODO 构建倒排索引表
               //使用这个字段的名字作为倒排索引表的表名
               val tableName = s"test:${relationField}"
               //使用这个字段的值作为倒排索引表的ROWKEY
               val rowkey = fieldValue
               //使用这条数据的MAC作为这条数据的value
               val indexValue = phone_mac
               //使用 字段值 的hash 作为版本号
               val relation_put = new Put(rowkey.getBytes())
               val index_versionNum = phone_mac.hashCode & Integer.MAX_VALUE
               relation_put.addColumn("cf".getBytes(),Bytes.toBytes("phone_mac"),index_versionNum,Bytes.toBytes(indexValue))
               HBaseInsertHelper.put(tableName,relation_put)
             }
          })
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()

  }



  //初始化hbase关联表
  def initRelationTables(): Unit ={
    val relationTable = s"test:relation"
    HBaseTableUtil.createTable(relationTable,"cf",true,-1,100,true,SpiltRegionUtil.getSplitKeysBydinct)
    println("创建表" + relationTable )
    //创建倒排索引表
    relationFields.foreach(field=>{
      //自动化    配置   字段 => 分区方法
      if(field.equals("phone")){
        HBaseTableUtil.createTable(s"test:${field}","cf",true,-1,100,true,SpiltRegionUtil.getSplitKeysByNumber)
      }else{
        HBaseTableUtil.createTable(s"test:${field}","cf",true,-1,100,true,SpiltRegionUtil.getSplitKeysBydinct)
        println("创建表" + s"test:${field}" )
      }
    })
  }

  /**
    * 清空表
    */
  def clearRelationTable(): Unit ={
    val relationTable = s"test:relation"
    HBaseTableUtil.deleteTable(relationTable)
    //创建倒排索引表
    relationFields.foreach(field=>{
      HBaseTableUtil.deleteTable(s"test:${field}")
    })
  }


}
