package com.hjx.weblog.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.hjx.weblog.flume.constant.ConstantFields;
import com.hjx.weblog.flume.service.DataCheck;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Axin in 2019/11/13 21:58
 */
public class DataCleanInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //数据解析
        if (event == null) {
            return null;
        }
        Event eventNew = new SimpleEvent();
        Map<String, String> headers = event.getHeaders();
        // 文件名中含有数据类型，这个地方我们需要通过数据类型去获取对应数据类型的元字段
        //数据类型_来源_UUID.txt
        //wechat_source1_1111111111.txt
        String fileName = headers.get(ConstantFields.FILENAME);
        String absolute_filename = headers.get(ConstantFields.ABSOLUTE_FILENAME);
        String line = new String(event.getBody(),Charsets.UTF_8);
        //TODO  将line 转为map
        //获取数据类型
        //TODO 还需要增加清洗规则
        Map<String, String> map = DataCheck.txtParseAndValidation(fileName, absolute_filename, line);
        //过滤掉为空的数据
        if(map == null){
            return eventNew;
        }
        String json = JSON.toJSONString(map);
        eventNew.setBody(json.getBytes());
        return eventNew;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> listEvent = new ArrayList<>();
        list.forEach(event->{
            Event enentNew = intercept(event);
            listEvent.add(enentNew);
        });
        return listEvent;
    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public void configure(Context context) {
        }
        @Override
        public Interceptor build() {
            return new DataCleanInterceptor();
        }
    }

    @Override
    public void close() {

    }
}
