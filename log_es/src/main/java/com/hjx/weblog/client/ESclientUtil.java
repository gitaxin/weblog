package com.hjx.weblog.client;


import com.hjx.weblog.common.config.ConfigUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Created by Axin in 2019/11/20 22:33
 */
public class ESclientUtil {


    private static final Logger logger = Logger.getLogger(ESclientUtil.class);

    private static final String esConfigpath = "test/es/es_cluster.properties";


    private volatile static TransportClient client;

    private ESclientUtil(){

    }

    //ES配置
    private static Properties properties;
    /**
     * API 官方文档
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.2/transport-client.html
     *
     * index 及mapping
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.2/java-admin-indices.html
     *
     * ES 每个分片存的是部分数据
     */


    //通过配置获取主机名和端口
    static {
        properties = ConfigUtil.getInstance().getProperties(esConfigpath);
    }

    //创建ES客户端
    public static TransportClient getClient(){

        String myClusterName = properties.get("es.cluster.name").toString();
        String host1 = properties.get("es.cluster.nodes1").toString();
        Integer port = Integer.valueOf(properties.get("es.cluster.tcp.port").toString());



        if(client == null){
            synchronized (ESclientUtil.class){
                if(client == null){
                    try {

                        /**
                         * 解决：
                         * java.lang.IllegalStateException: availableProcessors is already set to [8], rejecting [8]
                         *添加如下配置
                         * System.setProperty("es.set.netty.runtime.available.processors", "false")
                         */
                        System.setProperty("es.set.netty.runtime.available.processors", "false");

                        Settings settings = Settings.builder()
                                .put("cluster.name", myClusterName).build();

                        client = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(host1),port));
                    } catch (UnknownHostException e) {
                        logger.error("获取ESClient失败", e);
                    }
                }
            }
        }

        return client;

    }


    public static void main(String[] args) {
        TransportClient instance = ESclientUtil.getClient();
        System.out.println(instance);
    }


}
