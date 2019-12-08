package com.hjx.weblog.hbase.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Axin in 2019/11/30 21:30
 */
public class HBaseConf implements Serializable {
    //读取HBASE配置文件
    //获取连接
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(HBaseConf.class);
    //配置路径
    private static final String HBASE_SITE = "test/hbase/hbase-site.xml";

    private volatile static HBaseConf hbaseConf;

    //hbase 配置文件
    private  Configuration configuration;
    //hbase 连接
    private volatile Connection conn;

    /**
     * 初始化HBaseConf的时候加载配置文件
     */
    private HBaseConf() {
        getHconnection();
    }

    /**
     * 单例 初始化HBaseConf
     * @return
     */
    public static HBaseConf getInstance() {
        if (hbaseConf == null) {
            synchronized (HBaseConf.class) {
                if (hbaseConf == null) {
                    hbaseConf = new HBaseConf();
                }
            }
        }
        return hbaseConf;
    }


    //获取连接
    public Configuration getConfiguration(){
        if(configuration==null){
            configuration = HBaseConfiguration.create();
            //通过addResource方法把hbase配置文件加载进来
            configuration.addResource(HBASE_SITE);
            LOG.info("加载配置文件" + HBASE_SITE + "成功");
        }
        return configuration;
    }

    public BufferedMutator getBufferedMutator(String tableName) throws IOException {
        return getHconnection().getBufferedMutator(TableName.valueOf(tableName));
    }


    public Connection getHconnection(){

        if(conn==null){ //双重否定是为了提高性能，因为synchronized会浪费 性能，先判断 是否为空，再进入加载执行
            //加载配置文件
            getConfiguration();
            synchronized (HBaseConf.class) {
                if (conn == null) {
                    try {
                        conn = ConnectionFactory.createConnection(configuration);
                    } catch (IOException e) {
                        LOG.error(String.format("获取hbase的连接失败  参数为： %s", toString()), e);
                    }
                }
            }
        }
        return conn;
    }


}
