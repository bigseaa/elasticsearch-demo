package com.bigsea.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    @Value("${elasticsearch.hostlist}")
    private String hostlist;

    /**
     * 高等级客户端
     * @return RestHighLevelClient
     */
    @Bean
    public RestHighLevelClient restHighLevelClient() {
        // 解析hostlist配置信息
        String[] hostStrArray = hostlist.split(",");
        // 创建HttpHost数组，其中存放es主机和端口的配置信息
        HttpHost[] httpHostArray = new HttpHost[hostStrArray.length];
        // 创建RestHighLevelClient客户端
        for (int i = 0; i < hostStrArray.length; i++) {
            String item = hostStrArray[i];
            String[] itemArray = item.split(":");
            httpHostArray[i] = new HttpHost(itemArray[0], Integer.parseInt(itemArray[1]), "http");
        }
        // 创建RestHighLevelClient客户端
        return new RestHighLevelClient(RestClient.builder(httpHostArray));
    }

    /**
     * 传统客户端（不用）
     * @return RestClient
     */
    @Bean
    public RestClient restClient(){
        // 解析hostlist配置信息
        String[] hostStrArray = hostlist.split(",");
        // 创建HttpHost数组，其中存放es主机和端口的配置信息
        HttpHost[] httpHostArray = new HttpHost[hostStrArray.length];
        for (int i=0; i < hostStrArray.length; i++) {
            String item = hostStrArray[i];
            httpHostArray[i] = new HttpHost(item.split(":")[0], Integer.parseInt(item.split(":")
                    [1]), "http");
        }
        return RestClient.builder(httpHostArray).build();
    }
}
