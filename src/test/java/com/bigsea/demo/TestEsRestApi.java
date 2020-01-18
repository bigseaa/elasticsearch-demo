package com.bigsea.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试集群环境的低等级的客户端 版本6.5.4
 * 之所以成为低等级的客户端是因为操作方式偏向底层操作api，仅仅是将es提供的rest api接口做了一个简单的封装
 * 2020-01-18
 */
public class TestEsRestApi {
    private RestClient restClient;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 初始化连接elasticsearch集群
     */
    @Before
    public void init() {
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("192.168.1.207", 9200, "http"),
                new HttpHost("192.168.1.207", 9201, "http"),
                new HttpHost("192.168.1.207", 9202, "http"));
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                System.out.println("error -> " + node);
            }
        });
        this.restClient = restClientBuilder.build();
    }

    /**
     * 关闭与es的连接
     * @throws IOException IO异常
     */
    @After
    public void after() throws IOException {
        restClient.close();
    }

    /**
     * 查询es集群的状态
     * @throws IOException IO异常
     */
    @Test
    public void testGetInfo() throws IOException {
        Request request = new Request("GET", "/_cluster/state");
        request.addParameter("pretty","true");
        Response response = this.restClient.performRequest(request);
        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    /**
     * 新增数据
     * @throws IOException IO异常
     */
    @Test
    public void testCreateData() throws IOException {
        Request request = new Request("POST", "/mountain/something");
        Map<String, Object> data = new HashMap<>();
        data.put("id","10086");
        data.put("title","天上人间");
        data.put("price","9999");
        request.setJsonEntity(MAPPER.writeValueAsString(data));
        Response response = this.restClient.performRequest(request);
        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    /**
     * 根据id查询数据
     * @throws IOException IO异常
     */
    @Test
    public void testQueryById() throws IOException {
        Request request = new Request("GET", "/mountain/something/lWPCt28BWKTzGK4Bxl8j");
        Response response = this.restClient.performRequest(request);
        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    /**
     * 根据条件搜索数据
     * @throws IOException IO异常
     */
    @Test
    public void testSearchData() throws IOException {
        Request request = new Request("POST", "/mountain/something/_search");
        String searchJson = "{\"query\": {\"match\": {\"title\": \"人间\"}}}";
        request.setJsonEntity(searchJson);
        request.addParameter("pretty","true");
        Response response = this.restClient.performRequest(request);
        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));
    }
}
