package com.hjx.weblog.spark.streaming.kafka.hdfs

import java.util

import org.apache.commons.configuration.{CompositeConfiguration, ConfigurationException, PropertiesConfiguration}
import org.apache.spark.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Axin in 2019/11/25 21:07  
  */
object HiveConfig extends Serializable with Logging{


  //HDFS根路径
  var hive_root_path = "/user/hive/external/"
  val hiveFieldPath = "test/es/mapping/fieldmapping.properties"
  var config: CompositeConfiguration = null
  //通过去读配置文件 动态生成建表语句
  //获取所有表
  var tables: java.util.List[_] = null
  //获取所有表字段
  var tableFieldsMap:java.util.Map[String,java.util.HashMap[String,String]] = null
  //获取建表语句
  var hiveTableSQL:java.util.Map[String,String] = null
  //获取分区建表语句
  var hiveTableByDaySQL:java.util.Map[String,String] = null
  //mapSchema
  var mapSchema:java.util.Map[String,StructType] = null

  initParams

  def initParams(): Unit = {
    println("======================================config===============================")
    config = HiveConfig.readCompositeConfiguration(hiveFieldPath)
    config.getKeys.foreach(key => {
      println(key + ":" + config.getProperty(key.toString))
    })

    println("======================================tables===============================")
    tables = config.getList("tables")
    tables.foreach(table =>{
      println(table)
    })

    println("======================================tableFieldsMap===============================")
    tableFieldsMap = HiveConfig.getKeysByType()
    tableFieldsMap.foreach(x =>{
      println(x)
    })

    println("======================================hiveTableSQL===============================")
    hiveTableSQL = HiveConfig.getHiveTables()
    hiveTableSQL.foreach(x =>{
      println(x)
    })

    println("======================================hiveTableByDaySQL===============================")
    hiveTableByDaySQL = HiveConfig.getHiveTablesByDay()
    hiveTableByDaySQL.foreach(x =>{
      println(x)
    })

    println("======================================mapSchema===============================")
    mapSchema = HiveConfig.getSchema()
    mapSchema.foreach(x=>{
      println(x)
    })
  }

  def main(args: Array[String])= {

  }
  /**
    * 初始化schema
    */
  def getSchema():java.util.Map[String,StructType] ={

    //val schema = StructType(fields)
    //schema = StructType(StructField(name,StringType,true), StructField(age,StringType,true))
    //创建DF首先需要 1 rowRDD  2 schema
    //val peopleDF = spark.createDataFrame(rowRDD, schema)
    val mapStructType = new java.util.HashMap[String,StructType]

    for(table <-tables){
      //遍历所有的字段  构造StructField  然后将所有的StructField 放入到arrayStructFields中
      var arrayStructFields = ArrayBuffer[StructField]()
      //获取所有类型字段
      //(qq,{qq.imsi=string, qq.id=string, qq.send_message=string})
      val tableFields = tableFieldsMap.get(table)
      //qq.imsi=string, qq.id=string, qq.send_message=string
      val keyIterator =  tableFields.keySet().iterator()
      //遍历所有字段 拼装StructField
      while (keyIterator.hasNext){
        //获取字段
        val key = keyIterator.next()
        //获取字段 类型
        val fieldType = tableFields.get(key)
        val field = key.split("\\.")(1)

        fieldType match {
          case "string" => arrayStructFields += StructField(field,StringType,true)
          case "long" => arrayStructFields += StructField(field,StringType,true)
          case "double" => arrayStructFields += StructField(field,StringType,true)
          case _ =>
        }
      }
      //fields: Seq[StructField]
      val schema = StructType(arrayStructFields)
      //最终是为了创建schema  构建schema需要
      mapStructType.put(table.toString,schema)
    }
    mapStructType
  }


  /**
    * 构建HIVE 语句
    */
  def getHiveTables(): util.HashMap[String,String] ={
    val hiveTableSqlMap = new util.HashMap[String,String]()
    //对所有的表进行遍历
    tables.foreach(table=>{
      //TODO 开始构造SQL语句  拼接的方式
      var sql:String = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("
      //继续拼接字段

      //获取所有table的字段
      val fields = config.getKeys(table.toString)
      fields.foreach(tableField =>{
        //获取字段
        val field = tableField.toString.split("\\.")(1)
        //获取字段 类型
        val fieldType = config.getProperty(tableField.toString)
        sql = sql + field
        //拼字段类型  做字段类型转换
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })
      //去掉最后资格字段的逗号
      sql = sql.substring(0,sql.length -1)
      // 拼接存储格式和存储位置  hdfs路径
      sql = sql + s")STORED AS PARQUET LOCATION '${hive_root_path}${table}'"
      hiveTableSqlMap.put(table.toString,sql)
    })
    hiveTableSqlMap
  }


  /**
    * 构建HIVE 语句
    */
  def getHiveTablesByDay(): util.HashMap[String,String] ={
    val hiveTableSqlMap = new util.HashMap[String,String]()
    //对所有的表进行遍历
    tables.foreach(table=>{
      //TODO 开始构造SQL语句  拼接的方式
      var sql:String = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${table} ("
      //继续拼接字段
      //获取所有table的字段
      val fields = config.getKeys(table.toString)
      fields.foreach(tableField =>{
        //获取字段
        val field = tableField.toString.split("\\.")(1)
        //获取字段 类型
        val fieldType = config.getProperty(tableField.toString)
        sql = sql + field
        //拼字段类型  做字段类型转换
        fieldType match {
          case "string" => sql = sql + " string,"
          case "long" => sql = sql + " string,"
          case "double" => sql = sql + " string,"
          case _ =>
        }
      })
      //去掉最后资格字段的逗号
      sql = sql.substring(0,sql.length -1)
      // 拼接存储格式和存储位置  hdfs路径
      sql = sql + s") partitioned BY(year string,month string,day string) STORED AS PARQUET LOCATION '${hive_root_path}${table}'"
      hiveTableSqlMap.put(table.toString,sql)
    })
    hiveTableSqlMap
  }




  /**
    * 读取hive 字段配置文件
    * @param path
    * @return
    */
  def readCompositeConfiguration(path: String): CompositeConfiguration = {
    logInfo("加载配置文件 " + path)
    //多配置工具
    val compositeConfiguration = new CompositeConfiguration
    try {
      val configuration = new PropertiesConfiguration(path)
      compositeConfiguration.addConfiguration(configuration)
    } catch {
      case e: ConfigurationException => {
        logError("加载配置文件 " + path + "失败", e)
      }
    }
    logInfo("加载配置文件" + path + "成功。 ")
    compositeConfiguration
  }

  /**
    * 获取table-字段 对应关系
    * 使用 util.Map[String,util.HashMap[String, String结构保存
    * @return
    */
  def getKeysByType(): util.Map[String, util.HashMap[String, String]] = {
    val map = new util.HashMap[String, util.HashMap[String, String]]()
    //wechat, mail, qq
    val iteratorTable = tables.iterator()
    //对每个表进行遍历
    while (iteratorTable.hasNext) {
      //使用一个MAP保存一种对应关系  一个QQ = [field1,field2]
      val fieldMap = new util.HashMap[String, String]()
      //获取一个表
      val table: String = iteratorTable.next().toString
      //获取这个表的所有字段
      //TODO  获取所有以table开头的健
      val fields = config.getKeys(table)
      //将每种表的私有字段放到map中去
      while (fields.hasNext) {
        val field = fields.next().toString
        fieldMap.put(field, config.getString(field))
      }
      map.put(table, fieldMap)
    }
    map
  }



}
