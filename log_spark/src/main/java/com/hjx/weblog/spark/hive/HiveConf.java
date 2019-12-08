package com.hjx.weblog.spark.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.Iterator;
import java.util.Map;


public class HiveConf {

    //private static String DEFUALT_CONFIG = "spark/hive/hive-server-config";
    private static HiveConf hiveConf;
    private static HiveContext hiveContext;

    private HiveConf(){

    }
    public static HiveConf getHiveConf(){
        if(hiveConf==null){
            synchronized (HiveConf.class){
                if(hiveConf==null){
                    hiveConf=new  HiveConf();
                }
            }
        }
        return hiveConf;
    }

    public static HiveContext getHiveContext(SparkContext sparkContext){
        if(hiveContext==null){
            synchronized (HiveConf.class){
                if(hiveContext==null){

                    //系统加载本地执行
                    System.load("D:\\hadoop-2.7.2\\bin\\hadoop.dll");
                    System.load("D:\\hadoop-2.7.2\\bin\\winutils.exe");

                    //构建hiveContext
                    hiveContext = new  HiveContext(sparkContext);

                    //加载集群配置文件
                    Configuration conf = new Configuration();

                    conf.addResource("test/spark/hive/hive-site.xml");
                    conf.addResource("test/spark/hive/core-site.xml");
                    conf.addResource("test/spark/hive/hdfs-site.xml");
                    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> next = iterator.next();
                        hiveContext.setConf(next.getKey(), next.getValue());
                    }
                    hiveContext.setConf("spark.sql.parquet.mergeSchema", "true");
                }
            }
        }
        return hiveContext;
    }



}
