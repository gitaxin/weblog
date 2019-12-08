package com.hjx.weblog.flume.sink;

import com.google.common.base.Throwables;
import com.hjx.weblog.producer.StringProducer;
import org.apache.commons.io.Charsets;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

/**
 * Created by Axin in 2019/11/13 22:07
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = Logger.getLogger(KafkaSink.class);
    //定义kafka topoc
    private String kafkaTopic;


    @Override
    public void configure(Context context) {
        //读取kafka topic
        kafkaTopic = context.getString("kafkaTopic");
    }



    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();
            if(event == null){
                txn.rollback();
                return Status.BACKOFF;
            }//实现将event 推送到kafka
            String line = new String(event.getBody(),Charsets.UTF_8);
            //TODO 需要实现推送数据到kafka
            logger.info("==================" + line);
            StringProducer.producer(kafkaTopic,line);
            txn.commit();
            status = Status.READY;
        } catch (Exception e) {
            txn.rollback();
            status = Status.BACKOFF;
            throw Throwables.propagate(e);
            //if(e instanceof Error){
            //    throw (Error)t
            //}
        }finally {
            //如果不关闭，会报以下错误
            //java.lang.IllegalStateException:begin() when transaction is COMPLETED!
            if(txn!=null){
                txn.close();
            }
        }
        return status;
    }


    /**
     * java.lang.IllegalStateException:begin() when transaction is COMPLETED!
     */

}
