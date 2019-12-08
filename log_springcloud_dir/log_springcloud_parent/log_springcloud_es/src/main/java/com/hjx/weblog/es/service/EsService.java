package com.hjx.weblog.es.service;

import com.hjx.weblog.es.jest.jestservice.JestService;
import com.hjx.weblog.es.jest.jestservice.ResultParse;
import io.searchbox.client.JestClient;
import io.searchbox.core.SearchResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-09-04 22:24
 */
@Service
public class EsService {


    /**
     * 根据MAC获取轨迹信息
     * @param mac
     * @return
     */
    public List<Map<String, Object>> getLocus(String mac){

        List<Map<String, Object>> maps = null;
        //實現查詢
        // 构建 jestClient 客户端
        JestClient jestClient = null;

        String [] includes = new String[]{"latitude","longitude","collect_time"};
        try {
            jestClient = JestService.getJestClient();
            //查所有索引
            SearchResult search = JestService.search(jestClient,
                    "",
                    "",
                    "phone_mac.keyword",
                     mac,
                    "collect_time",
                    "asc",
                    1,
                    1000,
                    includes);
            maps = ResultParse.parseSearchResultOnly(search);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JestService.closeJestClient(jestClient);
        }
        return maps;
    }


    public List<Map<String, Object>> getBaseInfo(String indexName,
                                                 String typeName,
                                                 String sortField,
                                                 String sortValue,
                                                 int pageNumber,
                                                 int pageSize){

        List<Map<String, Object>> maps = null;
        //實現查詢
        // 构建 jestClient 客户端
        JestClient jestClient = null;
        try {
            jestClient = JestService.getJestClient();
            SearchResult search = JestService.search(jestClient,
                    indexName,
                    typeName,
                    "",
                    "",
                    sortField,
                    sortValue,
                    pageNumber,
                    pageSize);
            maps = ResultParse.parseSearchResultOnly(search);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JestService.closeJestClient(jestClient);
        }
        return maps;
    }
}
