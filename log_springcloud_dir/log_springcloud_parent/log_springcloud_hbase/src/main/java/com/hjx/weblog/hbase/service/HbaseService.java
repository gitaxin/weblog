package com.hjx.weblog.hbase.service;


import com.hjx.weblog.hbase.entity.HBaseCell;
import com.hjx.weblog.hbase.entity.HBaseRow;
import com.hjx.weblog.hbase.extractor.MultiVersionRowExtrator;
import com.hjx.weblog.hbase.extractor.SingleColumnMultiVersionRowExtrator;
import com.hjx.weblog.hbase.search.HBaseSearchService;
import com.hjx.weblog.hbase.search.HBaseSearchServiceImpl;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-09-03 22:20
 */
@Service
public class HbaseService {


    @Resource
    private HbaseService hbaseService;

    /**
     * 查询倒排索引  获取总关联表rowkey rowkey就是MAC
     * @param table
     * @param indexRowkey
     * @param versions
     * @return
     */
    public Set<String> getSingleColumn(String table, String indexRowkey,int versions){
        Set<String> relationKeySet = null;
        try {
            HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
            Get get = new Get(Bytes.toBytes(indexRowkey));
            get.setMaxVersions(versions);
            //查一列的所有版本
            SingleColumnMultiVersionRowExtrator singleColumnMultiVersionRowExtrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(), "phone_mac".getBytes(), new HashSet<>());
            relationKeySet = hBaseSearchService.search(table, get, singleColumnMultiVersionRowExtrator);
            System.out.println(relationKeySet);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return relationKeySet;
    }


    /**
     *  通过任意一个条件查询出所有  关联信息
     * @param field
     * @param fieldValue
     * @return
     */
    public Map<String,List<String>> getRelation(String field, String fieldValue){

        Map<String,List<String>> map = new HashMap<>();
        //查倒排表
        String table = "test:"+field;  //表名
        String indexRowkey = fieldValue;
        //返回所有主关联表的rowkey
        Set<String> singleColumn = hbaseService.getSingleColumn(table, indexRowkey, 1000);

        //根据rowkey查总关联表
        List<Map<String, String>> search = null;
        // 组装list<Get>
        List<Get> list = new ArrayList<>();
        singleColumn.forEach(mac->{
            Get get = new Get(mac.getBytes());
            //需要多版本
            try {
                get.setMaxVersions(1000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            list.add(get);
        });

        try {
            HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
            List<HBaseRow> searchResult = hBaseSearchService.search("test:relation", list, new MultiVersionRowExtrator());

            //多所有的cell遍历  将相同key的数据放到一个list里面去
            searchResult.forEach(hbaseRow ->{
                Map<String, Collection<HBaseCell>> cellMap = hbaseRow.getCell();
                cellMap.forEach((key,value)->{
                    List<String> listValue = new ArrayList<>();
                    value.forEach(x->{
                        listValue.add(x.toString());
                    });
                    map.put(key,listValue);
                });
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

}
