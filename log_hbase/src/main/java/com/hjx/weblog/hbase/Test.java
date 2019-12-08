package com.hjx.weblog.hbase;


import com.hjx.weblog.hbase.extractor.MapRowExtrator;
import com.hjx.weblog.hbase.search.HBaseSearchService;
import com.hjx.weblog.hbase.search.HBaseSearchServiceImpl;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-08-30 21:22
 */
public class Test {
    public static void main(String[] args) throws Exception {
        HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
        List<Get> list = new ArrayList<Get>();
        Get get = new Get(Bytes.toBytes("17609765432"));
        list.add(get);
        List<Map<String, String>> search = hBaseSearchService.search("test:phone", list, new MapRowExtrator());
        search.forEach(x->{
            System.out.println(x);
        });
    
    }
}
