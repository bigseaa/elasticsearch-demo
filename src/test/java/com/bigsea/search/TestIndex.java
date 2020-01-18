/*
package com.bigsea.search;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;

import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestIndex {
    @Autowired
    RestHighLevelClient client;

    @Autowired
    RestClient restClient;

    */
/**
     * 创建索引库
     * @throws IOException
     *//*

    @Test
    public void testCreateIndex() throws IOException {
        // 创建索引请求对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("bigseatest");
        // 设置分片与副本的数量
        createIndexRequest.settings(Settings.builder().put("number_of_shards",1)
                .put("number_of_replicas",0));
        // 指定映射

        createIndexRequest.mapping("{\n" +
                "\t\"properties\": {\n" +
                "\t\t\"bookname\":{\n" +
                "\t\t\t\"type\":\"keyword\"\n" +
                "\t\t},\n" +
                "\t\t\"description\":{\n" +
                "\t\t\t\"type\":\"text\",\n" +
                "\t\t\t\"analyzer\":\"ik_max_word\",\n" +
                "\t\t\t\"search_analyzer\":\"ik_smart\"\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}", XContentType.JSON);
        // 操作索引的客戶端
        IndicesClient indices = client.indices();
        // 创建索引库
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
        // 得到响应
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    */
/**
     * 删除索引库
     * @throws IOException
     *//*

    @Test
    public void testDeleteIndex() throws IOException {
        // 删除索引请求对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("bigseatest");
        // 操作索引的客戶端
        IndicesClient indices = client.indices();
        // 執行刪除
        AcknowledgedResponse deleteResponse = indices.delete(deleteIndexRequest, RequestOptions.DEFAULT);
        // 得到响应
        boolean acknowledged = deleteResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    */
/**
     * 添加文档
     * @throws IOException
     *//*

    @Test
    public void testAddDoc() throws IOException {
        // 准备json数据
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("bookname", "spring源码分析");
        jsonMap.put("description", "本书主要以大量源码的分析与实例深入讲解了spring，能给读者极大的提升");
        // 索引请求对象
        IndexRequest indexRequest = new IndexRequest("bigseatest");
        // 指定索引文档内容
        indexRequest.source(jsonMap);
        // 索引响应对象
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        // 获取响应结果
        DocWriteResponse.Result result = indexResponse.getResult();
        System.out.println(result);
    }

    */
/**
     * 查询文档
     * @throws IOException
     *//*

    @Test
    public void testGetDoc() throws IOException {
        GetRequest getRequest = new GetRequest(
                "bigseatest",
                "hEgtMWsBlmODdCaori-F");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        boolean exists = getResponse.isExists();
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println(sourceAsMap);
    }

    */
/**
     * 更新文档
     * @throws IOException
     *//*

    @Test
    public void updateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("bigseatest", "hEgtMWsBlmODdCaori-F");
        Map<String, Object> map = new HashMap<>();
        map.put("bookname", "spring源码分析2019");
        updateRequest.doc(map);
        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
        RestStatus status = update.status();
        System.out.println(status);
    }

    */
/**
     * 根据id删除文档
     * @throws IOException
     *//*

    @Test
    public void testDelDoc() throws IOException {
        //删除索引请求对象
        DeleteRequest deleteRequest = new DeleteRequest("bigseatest","hEgtMWsBlmODdCaori-F");
        //响应对象
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        //获取响应结果
        DocWriteResponse.Result result = deleteResponse.getResult();
        System.out.println(result);
    }
}
*/
