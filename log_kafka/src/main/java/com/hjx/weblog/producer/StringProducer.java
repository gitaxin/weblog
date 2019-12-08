package com.hjx.weblog.producer;

import com.hjx.weblog.config.KafkaConfig;
import kafka.javaapi.producer.Producer;


import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.applet.Main;

/**
 * Created by Axin in 2019/11/14 22:19
 *
 * Kafka 生产者
 *
 * 查看所有topic
 * kafka-topics --list --zookeeper bigdata002:2181
 *
 * 创建topic
 * kafka-topics --create --zookeeper bigdata002:2181 --replication-factor 1 --partitions 3 --topic chl_test0
 *
 * 消费topic
 * kafka-console-consumer --zookeeper bigdata002:2181 --topic chl_test0 --from-beginning
 *
 *
 */
public class StringProducer {

    private static final Logger log = LoggerFactory.getLogger(StringProducer.class);

    //写一个生产者方法，提供给flume调用
    public static void producer(String topic,String recourd) {

        //构造new ProducerConfig()
        ProducerConfig producerConfig = KafkaConfig.getInstance().getProducerConfig();
        Producer<String, String> produce = new Producer<>(producerConfig);

        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(topic, recourd);
        produce.send(keyedMessage);
        log.info("发送kafka消息到{}成功===>{}",topic,recourd);




    }




    public static void main(String[] args) {
        StringProducer.producer("chl_test2","1234456");
    }





}
