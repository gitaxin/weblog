package com.hjx.weblog.redis.client;


import com.hjx.weblog.common.config.ConfigUtil;
import com.hjx.weblog.common.constant.EnvConstant;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;
import java.util.Properties;

/**
 * Created by Axin in 2019/11/22 21:50
 */
public class JedisUtil {

    private static final Logger LOG = Logger.getLogger(JedisUtil.class);


    private static Properties redisConf ;
    private static final String redisConfPath = EnvConstant.DEV + "/redis/redis.properties";

    static {
        redisConf = ConfigUtil.getInstance().getProperties(redisConfPath);
    }

    public static Jedis getJedis(int db){
        Jedis jedis = JedisUtil.getJedis();
        if(jedis!=null){
            jedis.select(db);
        }
        return jedis;
    }


    public static void close(Jedis jedis){
        if(jedis!=null){
            jedis.close();
        }
    }


    /**
     *  重试5次 还未获取 中断
     * @return
     */
    public static Jedis getJedis(){
        int timeoutCount = 0;
        while (true) {// 如果是网络超时则多试几次
            try
            {
                Jedis jedis = new Jedis(redisConf.get("redis.hostname").toString(),
                        Integer.valueOf(redisConf.get("redis.port").toString()));
                return jedis;
            } catch (Exception e)
            {
                if (e instanceof JedisConnectionException || e instanceof SocketTimeoutException)
                {
                    timeoutCount++;
                    LOG.warn("获取jedis连接超时次数:" +timeoutCount);
                    if (timeoutCount > 4)
                    {
                        LOG.error("获取jedis连接超时次数a:" +timeoutCount);
                        LOG.error(null,e);
                        break;
                    }
                }else
                {
                    LOG.error("getJedis error", e);
                    break;
                }
            }
        }
        return null;
    }

}
