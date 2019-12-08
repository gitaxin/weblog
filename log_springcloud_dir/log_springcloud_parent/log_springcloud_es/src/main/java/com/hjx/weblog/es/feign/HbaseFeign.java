package com.hjx.weblog.es.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Set;

/**
 * Created by Axin in 2019/12/7 21:55
 */

@FeignClient(name = "log-springcloud-hbasequery")
public interface HbaseFeign {


    @ResponseBody
    @RequestMapping(value = "/hbase/getSingleColumn",method = {RequestMethod.GET})
    Set<String> getSingleColumn(@RequestParam(name="field") String field,
                                       @RequestParam(name="fieldValue") String fieldValue);
}
