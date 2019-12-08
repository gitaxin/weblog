package com.hjx.weblog.config;

import com.hjx.weblog.common.config.ConfigUtil;
import com.hjx.weblog.common.constant.EnvConstant;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by Axin in 2019/11/14 22:25
 */
public class KafkaConfig {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfig.class);
    private static final String DEFUALT_CONFIG_PATH = EnvConstant.DEV + "/kafka/kafka-server-config.properties";
    private volatile static KafkaConfig kafkaConfig = null;

    //kafka ProducerConfig
    private ProducerConfig config;
    // 获取kafka 配置文件
    private Properties properties;

    private KafkaConfig() throws IOException{
        try {
            properties = ConfigUtil.getInstance().getProperties(DEFUALT_CONFIG_PATH);
        } catch (Exception e) {
            IOException ioException = new IOException();
            ioException.addSuppressed(e);
            throw ioException;
        }
        //实例化的时候构造了 ProducerConfig
        config = new ProducerConfig(properties);
    }

    public static KafkaConfig getInstance(){

        if(kafkaConfig == null){
            synchronized (KafkaConfig.class) {
                if(kafkaConfig == null){
                    try {
                        kafkaConfig = new KafkaConfig();
                    } catch (IOException e) {
                        LOG.error("实例化kafkaConfig失败", e);
                    }
                }
            }
        }
        return kafkaConfig;
    }

    public ProducerConfig getProducerConfig(){
        return config;
    }


    public static void main(String[] args) {
        ProducerConfig producerConfig = KafkaConfig.getInstance().getProducerConfig();
    }
}
