package com.hjx.weblog.es.controller;

import com.hjx.weblog.es.feign.HbaseFeign;
import com.hjx.weblog.es.jest.jestservice.JestService;
import com.hjx.weblog.es.jest.jestservice.ResultParse;
import com.hjx.weblog.es.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Axin in 2019/12/5 21:12
 */
@Controller
@RequestMapping("/es")
public class EsController {

    //引入hbase feign(微服务调用)
    @Resource
    HbaseFeign hbaseFeign;


    @Resource
    private EsService esService;

    /**
     *
     * @param indexName     索引名
     * @param typeName      类型名
     * @param sortField     排序字段
     * @param sortValue      排序方式
     * @param pageNumber    页数
     * @param pageSize      没页数据大小
     * @return
     */
    //通用查询
    //ES一个基础查询
    //1 根据单个检索条件
    //2 分页  一页查多少条数据  查询条数  1.1000  2 分页 2. 10
    //3 排序  时间排序
    @ResponseBody
    @RequestMapping(value = "/getBaseInfo", method = {RequestMethod.GET, RequestMethod.POST})
    public List<Map<String, Object>> getBaseInfo(@RequestParam(name="indexName") String indexName,
                                                 @RequestParam(name="typeName") String typeName,
                                                 @RequestParam(name="sortField") String sortField,
                                                 @RequestParam(name="sortValue") String sortValue,
                                                 @RequestParam(name="pageNumber") String pageNumber,
                                                 @RequestParam(name="pageSize") String pageSize){

        int pageNumber1 = Integer.valueOf(pageNumber);
        int pageSize1 = Integer.valueOf(pageSize);

        return esService.getBaseInfo(indexName,typeName,sortField,sortValue,pageNumber1,pageSize1);
    }


        /**
         * GET _search
         * {
         *   "from": 0,
         *   "size": 1000,
         *   "_source": [
         *       "latitude",
         *       "longitude",
         *       "collect_time"
         *     ],
         *   "query": {
         *     "bool": {
         *       "must": [
         *         {
         *           "match_phrase": {
         *             "phone_mac": "aa-aa-aa-aa-aa-aa"
         *           }
         *
         *         }
         *       ]
         *     }
         *   }
         *   , "sort": [
         *     {
         *       "collect_time": {
         *         "order": "asc"
         *       }
         *     }
         *   ]
         * }
         */

    /**
     * 根据单个条件，查询所有轨迹数据， 可使用webgis技术展现
     * @param field
     * @param fieldValue
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/getLocus", method = {RequestMethod.GET, RequestMethod.POST})
    public List<Map<String, Object>> getLocus(@RequestParam(name="field") String field,
                                                 @RequestParam(name="fieldValue") String fieldValue){


        //一个手机可能有多个mac，所以要先通过hbase查询到该手机号的所有mac
        //第一步：首先要获取到手机的所有Mac >> 从hbase中查。hbase需要提供一个方法，通过一个字段，去找这个字段对应的所有Mac

        Set<String> macs = hbaseFeign.getSingleColumn(field, fieldValue);
        Iterator<String> iterator = macs.iterator();
        //while(iterator.hasNext()){
            String mac = iterator.next();
            System.out.println("调用hbase服务的getSingleColumn方法成功" + mac);
        //}


        //第二步：从es中，根据所有Mac查找所有index 所有type 中查找轨迹
        List<Map<String, Object>> locus = esService.getLocus(mac);

        return locus;
    }



    /**
     * 根据任意数据类型条件查找轨迹数据
     * 测试jestAPI是否正常
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/test", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, Object> getTest(){
        Map<String, Object> stringObjectMap = null;
        try {
            // 构建 jestClient 客户端
            JestClient jestClient = JestService.getJestClient();
            // 调用
            JestResult jestResult = JestService.get(jestClient, "wechat_20190508", "wechat_20190508", "5cfe4363d3224b83bb10dd56a896dff4");
            // 使用ResultParse 解析器
            stringObjectMap = ResultParse.parseGet(jestResult);
            System.out.println(jestResult);
            System.out.println(stringObjectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringObjectMap;
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


}
