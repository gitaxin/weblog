package com.hjx.weblog.spark.warn.timer;

import com.hjx.weblog.redis.client.JedisUtil;
import com.hjx.weblog.spark.warn.dao.TZ_RuleDao;
import com.hjx.weblog.spark.warn.domain.TZ_RuleDomain;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;


import java.util.List;
import java.util.TimerTask;


public class SyncRule2Redis extends TimerTask {
    private static final Logger LOG = Logger.getLogger(SyncRule2Redis.class);
    @Override
    public void run() {
        LOG.error("开始同步mysql规则到REDIS");
        //1.首先是获取mysql中的所有规则  查规则表所有数据
        List<TZ_RuleDomain> ruleList = TZ_RuleDao.getRuleList();
        //2.将所有规则写入REDIS
        Jedis jedis15 = null;
        try {
            jedis15 = JedisUtil.getJedis(15);
            for (int i = 0; i <ruleList.size() ; i++) {
                TZ_RuleDomain rule = ruleList.get(i);
                String id = rule.getId()+"";
                String publisher = rule.getPublisher();
                String warn_fieldname = rule.getWarn_fieldname();
                String warn_fieldvalue = rule.getWarn_fieldvalue();
                String send_mobile = rule.getSend_mobile();
                String send_type = rule.getSend_type();
                //拼接redis key值
                String redisKey = warn_fieldname +":" + warn_fieldvalue;
                //通过redis hash结构   hashMap
                jedis15.hset(redisKey,"id",StringUtils.isNoneBlank(id) ? id : "");
                jedis15.hset(redisKey,"publisher",StringUtils.isNoneBlank(publisher) ? publisher : "");
                jedis15.hset(redisKey,"warn_fieldname",StringUtils.isNoneBlank(warn_fieldname) ? warn_fieldname : "");
                jedis15.hset(redisKey,"warn_fieldvalue",StringUtils.isNoneBlank(warn_fieldvalue) ? warn_fieldvalue : "");
                jedis15.hset(redisKey,"send_mobile",StringUtils.isNoneBlank(send_mobile) ? send_mobile : "");
                jedis15.hset(redisKey,"send_type",StringUtils.isNoneBlank(send_type) ? send_type : "");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JedisUtil.close(jedis15);
        }
        LOG.error("开始同步mysql规则到REDIS完成");
    }


    public static void main(String[] args) {
        SyncRule2Redis syncRule2Redis = new SyncRule2Redis();
        syncRule2Redis.run();
    }

}
