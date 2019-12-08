package com.hjx.weblog.spark.streaming.kafka.hdfs

import java.util

import com.hjx.weblog.common.time.TimeTranstationUtils
import com.hjx.weblog.spark.common.StreamingContextFactory
import com.hjx.weblog.spark.hive.{HdfsAdmin, HiveConf}
import com.hjx.weblog.spark.streaming.kafka.Spark_Kafka_ConfigUtil
import com.hjx.weblog.spark.streaming.kafka.hdfs.HiveConfig.hiveTableByDaySQL
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.JavaConversions._

/**
  * Created by Axin in 2019/11/24 17:47
  *
  *
  *
  * 异常
  * org.apache.hadoop.hive.ql.metadata.HiveException: MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException Permission denied: user=Axin, access=WRITE, inode="/user/hive":hive:hive:drwxrwxr-t
  * 解决方法一： 在hdfs-site.xml中添加以下配置，不起作用
  * <property>
  * <name>dfs.permissions</name>
  * <value>false</value>
  * </property>
  * 解决方法二：在CDH控制界面修改HDFS 的配置 :取消选择 dfs.permissions  不起作用
  * 解决方法三：将要操作的hdfs目录所有者修改为 Axin   解决
  * sudo -u hdfs hadoop fs -chown Axin /user/hive/
  *
  * 解决方法四：修改其他用户也具有修改权限(修使用hdfs用户执行命令) 解决
  * hdfs fs -chmod - R 777 hdfs://192.168.110.221:8020/user/hive/
  *
  */
object Kafka2hiveByDay extends Serializable with Logging{

  def main(args: Array[String]): Unit = {

    //定义topic
    val topics = "chl_test0"
    //首先从kafka读取数据
    val ssc = StreamingContextFactory.newSparkLocalStreamingContext("Kafka2hive", 5L, 2)
    //val ssc = StreamingContextFactory.newSparkStreamingContext("SparkstreamingKafka2esTest",10)
    //构建kafka参数
    val kafkaParams = Spark_Kafka_ConfigUtil.getKafkaParam(topics, "Kafka2hive")
    val kafkaManager = new KafkaManager(kafkaParams)
    val mapDS = kafkaManager.createJsonToJMapStringDirectStreamWithOffset(ssc, Set(topics))
      .map(map => {
        //TODO 值应该是时间的日期形式，通过时间戳转化
        // dayPartion是为了按天过滤数据
        val yyyyMMdd = TimeTranstationUtils.Date2yyyyMMdd(java.lang.Long.valueOf(map.get("collect_time") + "000"))
        map.put("dayPartion", yyyyMMdd)

        //分目录字段
        val year = yyyyMMdd.substring(0, 4) //年
        val month = yyyyMMdd.substring(4, 6)
        val day = yyyyMMdd.substring(6, 8)
        map.put("year", year)
        map.put("month", month)
        map.put("day", day)
        map
      })


    val sc = ssc.sparkContext
    //TODO 首先创建HIVE表  用HIVEcontext构建  自己拼接SQL建表语句
    //构建hiveConext环境变量
    val hiveContext = HiveConf.getHiveContext(sc)
    //hiveContext.setConf("spark.sql.parquet.mergeSchema", "true")
    //初始化HIVE表构建 可以直接写死，也可以动态拼接
    hiveTableByDaySQL.foreach(x=>{
      val sql = x._2
      hiveContext.sql(sql)
      println()
    })

    //QQ,EMAIL,WECHAT
    //mapDS里面包含三种数据类型
    //需要对mapDS按类别进行过滤
    HiveConfig.tables.foreach(table =>{
      // qq  这样就过滤出了table = QQ的所有数据
      val tableDS = mapDS.filter(line => {table.equals(line.get("table"))})
      tableDS.foreachRDD(rdd=>{


        //继续按天数拆分
        //索引名
        //拿到所有天数   //把所有数据拉到driver上
        val arrayDays = rdd.map(x => {
          x.get("dayPartion")
        }).distinct().collect()

        arrayDays.foreach(date => {
          //分目录字段
          val year = date.substring(0, 4) //年
          val month = date.substring(4, 6)
          val day = date.substring(6, 8)
          //将RDD转为DF
          val schema = HiveConfig.mapSchema.get(table)
          //所有字段名
          val schemaFields = schema.fieldNames
          println("date========" + date)
          //RDD转DF  dataFrame是以Row为单位的
          val rowRDD = rdd.filter(map => {
            date.equals(map.get("dayPartion"))
          }).repartition(1).map(map => {
            //把所有的MAP 转成Row结构
            //对每条数据遍历  将每条数据转为Row
            val listRow: java.util.ArrayList[Object] = new util.ArrayList[Object]()
            //对所有的字段遍历
            for (schemaField <- schemaFields) {
              listRow.add(map.get(schemaField))
            }
            Row.fromSeq(listRow)
          }).repartition(1)

          //DF写入HDFS
          val tableDF = hiveContext.createDataFrame(rowRDD, schema)
          tableDF.show(2)
          //将HDFS中的数据LOAD到HIVE表中
          val path_all = s"hdfs://bigdata001:8020${HiveConfig.hive_root_path}${table}/${year}/${month}/${day}"
          println("path_all=======" + path_all)
          //数据写入之前 判断目录是不是存在
          val exists = HdfsAdmin.get().getFs.exists(new Path(path_all))
          println("exists=========" + exists)
          //建立映射关系
          // if(!exists){  //不加判断  每次都会重新建立映射关系
          tableDF.write.mode(SaveMode.Append).parquet(path_all)
          //加载到HIVE  现在这个目录因为没有写入  所有这个目录还不存在
          println("======================开始加载数据到分区======================")
          hiveContext.sql(s"ALTER TABLE ${table} ADD IF NOT EXISTS PARTITION(year= '${year}',month='${month}',day='${day}') LOCATION '${path_all}'")
          // }else{
          //    tableDF.write.mode(SaveMode.Append).parquet(path_all)
          // }
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }


}
