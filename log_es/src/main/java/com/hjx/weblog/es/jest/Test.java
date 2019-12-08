package com.hjx.weblog.es.jest;


import com.hjx.weblog.es.jest.jestservice.JestService;
import com.hjx.weblog.es.jest.jestservice.ResultParse;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;

import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-09-04 20:49
 */
public class Test {
    public static void main(String[] args) {
        Map<String, Object> stringObjectMap = null;
        try {
            // 构建 jestClient 客户端
            JestClient jestClient = JestService.getJestClient();
            // 调用
            JestResult jestResult = JestService.get(jestClient, "qq_20190508", "qq_20190508", "c0a800e1d08040e6bef0d5f18c1d3974");
            // 使用ResultParse 解析器
            stringObjectMap = ResultParse.parseGet(jestResult);
            System.out.println(jestResult);
            System.out.println(stringObjectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(stringObjectMap.toString());

    }
}
