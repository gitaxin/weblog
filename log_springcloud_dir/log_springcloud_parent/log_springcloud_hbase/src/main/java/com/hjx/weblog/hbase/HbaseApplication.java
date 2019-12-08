package com.hjx.weblog.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

/**
 * Created by Axin in 2019/12/2 22:14
 *
 * http://localhost:8002
 */
@SpringBootApplication
@EnableEurekaClient
public class HbaseApplication {
    public static void main(String[] args) {
        //固定写法  启动这个服务
        SpringApplication.run(HbaseApplication.class,args);
    }
}
