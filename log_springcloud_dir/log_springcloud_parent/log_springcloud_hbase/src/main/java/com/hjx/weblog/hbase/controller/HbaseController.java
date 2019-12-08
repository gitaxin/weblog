package com.hjx.weblog.hbase.controller;


import com.hjx.weblog.hbase.extractor.MapRowExtrator;
import com.hjx.weblog.hbase.search.HBaseSearchService;
import com.hjx.weblog.hbase.search.HBaseSearchServiceImpl;
import com.hjx.weblog.hbase.service.HbaseService;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;


import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-09-03 21:17
 */
//@Controller一个控制器
@Controller
//路由
@RequestMapping("/hbase")
public class HbaseController {

    @Resource
    private HbaseService hbaseService;


    /**
     * 通过一种数据类型，寻找所有对应的主关联rowkey
     * @param field
     * @param fieldValue
     * @return
     */

    @ResponseBody
    @RequestMapping(value = "/getSingleColumn",method = {RequestMethod.GET})
    public Set<String> getSingleColumn(@RequestParam(name="field") String field,
                                   @RequestParam(name="fieldValue") String fieldValue){

        String hbaseTable = "test:" + field;
        return hbaseService.getSingleColumn(hbaseTable,fieldValue,1000);

    }



    /**
     * http://localhost:8002/hbase/getRelation?field=phone&fieldValue=18609765438
     * @param field
     * @param fieldValue
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/getRelation",method = {RequestMethod.GET,RequestMethod.POST})
    public Map<String, List<String>> getRelation(@RequestParam(name="field") String field,
                              @RequestParam(name="fieldValue") String fieldValue){

        System.out.println("field====" +field);
        System.out.println("fieldValue====" +fieldValue);
        Map<String, List<String>> relation = hbaseService.getRelation(field, fieldValue);

        System.out.println(relation.toString());
        System.out.println(relation.size());

        return relation;
    }








    /**
     *  这里可以返回界面想要的数据，我们在这里面实现  查询hbase,ES ,HIVE 等接口就行了
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/print",method = {RequestMethod.GET,RequestMethod.POST})
    public String printHello(){
        return "hello";
    }












    @ResponseBody
    @RequestMapping(value = "/hbaseTest",method = {RequestMethod.GET,RequestMethod.POST})
    public List<Map<String, String>> hbaseTest()
    {
        List<Map<String, String>> search = null;
        try {
            HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
            List<Get> list = new ArrayList<Get>();
            Get get = new Get(Bytes.toBytes("18609765432"));
            list.add(get);
            search = hBaseSearchService.search("test:phone", list, new MapRowExtrator());
            search.forEach(x->{
                System.out.println(x);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return search;
    }



}
