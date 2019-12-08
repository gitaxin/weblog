package com.hjx.weblog.eureka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * Created by Axin in 2019/12/2 21:51
 */

/**
 * http://localhost:8761
 */

//SpringBootApplication 注解说明这个应用是一个SpringBoot服务
@SpringBootApplication(exclude = {org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class})
//把他作为一个注册中心
@EnableEurekaServer
public class EurekaApplication {
    public static void main(String[] args) {
        //固定写法  启动这个服务
        SpringApplication.run(EurekaApplication.class,args);
    }
}