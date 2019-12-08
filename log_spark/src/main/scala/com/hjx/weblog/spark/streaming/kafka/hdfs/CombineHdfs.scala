package com.hjx.weblog.spark.streaming.kafka.hdfs

import com.hjx.weblog.spark.common.SparkContextFactory
import com.hjx.weblog.spark.hive.HdfsAdmin
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.Logging
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.JavaConversions._

/**
  * Created by Axin in 2019/11/27 20:58
  *
  * 定时合并小文件，每天执行一次
  * 使用linux系统命令crontab 命令定时执行，或使用其他任务调度工具 每天定时执行 前一天的数据
  * TODO 参考word文档 sparkstreaming-HIVE.docx
  */
object CombineHdfs extends  Serializable with Logging{



  def main(args: Array[String]): Unit = {

    val sparkContext = SparkContextFactory.newSparkLocalBatchContext("CombineHdfs",1)
    val sqlContext = new SQLContext(sparkContext)
    //遍历读取HDFS数据
    HiveConfig.tables.foreach(table=>{
      println(s"====================${table}===================================")
      //定义HDFS目录
      val table_path = s"hdfs://bigdata001:8020${HiveConfig.hive_root_path}${table}"
      //数据已经全部拿到了
      val tableDF =  sqlContext.read.load(table_path)
      //在合并文件还没写入原来HDFS目录之前  需要将原来的小文件索引拿到，
      // 然后写入，再删除小文件，，避免合并之后的文件写入HDFS目录和原来的小文件混在一起
      val fileSystem : FileSystem = HdfsAdmin.get().getFs
      //用一个正则把以 part开头的文件全部拿到
      val arrayFileStatuses = fileSystem.globStatus(new Path(table_path + "/part*"))
      //arrayFileStatuses.foreach(println(_))
      //FileUtil.stat2Paths 将status转为path
      val paths = FileUtil.stat2Paths(arrayFileStatuses)
      //文件路径已经全部拿到
      //合并写入原HDFS目录
      tableDF.repartition(1).write.mode(SaveMode.Append).parquet(table_path)
      println(s"合并写入${table_path}成功")
      //删除原来的小文件
      paths.foreach(path=>{
        fileSystem.delete(path)
        println(s"删除文件${path}成功")
      })
    })
  }
}
