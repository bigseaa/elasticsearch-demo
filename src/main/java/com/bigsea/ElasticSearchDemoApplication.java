package com.bigsea;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages={"com.bigsea"})//扫描类
public class ElasticSearchDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(ElasticSearchDemoApplication.class, args);
    }

}
