package com.hjx.weblog.flume.service;


import com.hjx.weblog.common.properties.DataTypeProperties;
import com.hjx.weblog.flume.constant.ErrorMapFields;
import com.hjx.weblog.flume.constant.MapFields;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-08-12 22:00
 */
public class DataCheck {
    private static final Logger logger = Logger.getLogger(DataCheck.class);

    static Map<String, ArrayList<String>> dataTypeMap;

    static {
        dataTypeMap = DataTypeProperties.dataTypeMap;
    }


    //定义解析 验证方法
    // 1 解析
    // 2 校验

    public static Map<String,String> txtParseAndValidation(String fileName,
                                                           String absolute_filename,
                                                           String line){

        Map map = new HashMap<String,String>();
        Map errorMap = new HashMap<String,Object>();
        //1.解析
        //文件名按"_"切分  wechat_source1_1111142.txt
        //wechat 数据类型
        //source1 数据来源
        //1111142  不让文件名相同
        String table = fileName.split("_")[0];
        //根据数据类型获取 类型对应的字段
        //fields = imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,username,phone,object_username,send_message,accept_message,message_time
        ArrayList<String> fields = dataTypeMap.get(table);
        String[] values = line.split("\t");

        //TODO 长度校验,数据解析
        //如果要映射上的话  必须保证字段长度 和line的长度相同
        if(fields.size() == values.length){
            for (int i = 0; i <values.length ; i++) {
                map.put(fields.get(i),values[i]);
            }
            //添加公共字段  唯一ID 数据类型 入库事件  文件名  文件绝对路径
            // map.put(SOURCE, source);
            map.put(MapFields.ID, UUID.randomUUID().toString().replace("-", ""));
            map.put(MapFields.TABLE, table.toLowerCase());
            map.put(MapFields.RKSJ, (System.currentTimeMillis() / 1000) + "");
            map.put(MapFields.FILENAME, fileName);
            map.put(MapFields.ABSOLUTE_FILENAME, absolute_filename);
        }else{
            //保存错误消息 最后会写入到ES中
            //写ES的时候  我需要能直接找到出问题的地方
            //记录下绝对路径，可以通过绝对路径直接找到对应的文件
            errorMap.put("leng_error","字段串长度不相等");
            errorMap.put(ErrorMapFields.LENGTH,"字段数不匹配" + fields.size() + "\t" + "实际应该是" + values.length);
            errorMap.put(ErrorMapFields.LATITUDE_ERROR,ErrorMapFields.LENGTH_ERROR_NUM);
            logger.info("字段数不匹配" + fields.size() + "\t" + "实际应该是" + values.length);
            //数据错误之后不能写入ES中
            //将map置为空，最后时候统一过滤为空的数据
            map = null;
        }

        //2.校验
        //定义一个校验工具类
        if(map !=null && map.size()>0 ){
            // 汇集到的所有错误信息
            errorMap = DataValidation.dataValidation(map);
        }
        //TODO 最后将errorMap 错误信息写入到ES中  方便错误查询  检正

        return map;
    }



}
