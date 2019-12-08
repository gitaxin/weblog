package com.hjx.weblog.es;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * Created by Axin in 2019/12/5 21:08
 */
@SpringBootApplication
@EnableEurekaClient
@EnableFeignClients
public class EsApplication {

    public static void main(String[] args) {
        SpringApplication.run(EsApplication.class);
    }
}
