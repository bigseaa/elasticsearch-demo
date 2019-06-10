package com.bigsea.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestSearch {
    @Autowired
    RestHighLevelClient client;

    @Autowired
    RestClient restClient;

    /**
     * 打印结果集
     * @param searchHits SearchHit[]
     */
    private void printResult(SearchHit[] searchHits) {
        int num = 0;
        for (SearchHit searchHit : searchHits) {
            num++;
            System.out.println("第" + num + "条数据:********");
            // String index = searchHit.getIndex();
            // String id = searchHit.getId();
            // float score = searchHit.getScore();
            String sourceAsString = searchHit.getSourceAsString();
            // search结果字符串
            System.out.println("result String" + sourceAsString);
            // search 结果map对象
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            double price = (double) sourceAsMap.get("price");
            String description = (String) sourceAsMap.get("description");
            System.out.println(name);
            System.out.println(price);
            System.out.println(description);
        }
    }
    /**
     * 搜索bigsea_demo索引中所有的doc
     * @throws IOException
     */
    @Test
    public void testSearchAll() throws IOException {
        // 基础设置
        SearchRequest searchRequest = new SearchRequest("bigsea_demo");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 搜索方式
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        // 返回字段过滤（可选）
        // 第一个参数为结果集包含哪些字段，第二个参数为结果集不包含哪些字段
        //searchSourceBuilder.fetchSource(new String[]{"name","description"}, new String[]{});

        // 发起请求，获取结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 打印结果集
        printResult(searchHits);
    }

    /**
     * 以分页查询的请求方式得到结果
     * @throws IOException
     */
    @Test
    public void testPageSearch() throws IOException {
        // 基础设置
        SearchRequest searchRequest = new SearchRequest("bigsea_demo");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 设置分页参数
        searchSourceBuilder.from(0).size(1);
        // 搜索方式
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        // 发起请求，获取结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 打印结果集
        printResult(searchHits);
    }

    /**
     * term query查询 该查询为精确查询（不会对查询条件进行分词），在查询时会以查询条件整体去匹配词库中的词（分词后的单个词）
     * @throws IOException
     */
    @Test
    public void testTermSearch() throws IOException {
        // 基础设置
        SearchRequest searchRequest = new SearchRequest("bigsea_demo");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 搜索方式
        searchSourceBuilder.query(QueryBuilders.termQuery("name", "spring源码"));
        searchRequest.source(searchSourceBuilder);
        // 发起请求，获取结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 打印结果集
        printResult(searchHits);
    }

    /**
     * 根据id进行精确查询（实际上是以id为terms）,注意：terms
     * @throws IOException
     */
    @Test
    public void testSearchById() throws IOException {
        // 基础设置
        SearchRequest searchRequest = new SearchRequest("bigsea_demo");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        List<String> idList = new ArrayList<>();
        idList.add("1");
        idList.add("2");
        // 搜索方式
        searchSourceBuilder.query(QueryBuilders.termsQuery("_id", idList));
        searchRequest.source(searchSourceBuilder);
        // 发起请求，获取结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 打印结果集
        printResult(searchHits);
    }

    /**
     * 根据关键字搜索
     * @throws IOException
     */
    @Test
    public void testMatchSearch() throws IOException {
        // 基础设置
        SearchRequest searchRequest = new SearchRequest("bigsea_demo");
        // 搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 匹配关键字

        // 写法一：会将"spring实战"分成两个词，只有要有一个匹配成功，则返回该文档(Operator.OR)
        //searchSourceBuilder.query(QueryBuilders.matchQuery("description", "spring实战").operator(Operator.OR));

        // 写法二:只要有两个词匹配成功，则返回文档（如果是3个词，则是0.7*3，向下取整得到2，匹配到两个词则返回文档）
        searchSourceBuilder.query(QueryBuilders.matchQuery("description", "spring微服务实战")
                .minimumShouldMatch("70%"));

        searchRequest.source(searchSourceBuilder);
        // 发起请求，获取结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 打印结果集
        printResult(searchHits);
    }

    /**
     * 根据关键字搜索（多个域）
     * @throws IOException
     */
    @Test
    public void testMultiMatchSearch() throws IOException {
        // 基础设置
        SearchRequest searchRequest = new SearchRequest("bigsea_demo");
        // 搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // matchQuery（以name、descrption两个域为搜索域，并且至少匹配到70%的词）
        MultiMatchQueryBuilder multiMatchQueryBuilder =
                QueryBuilders.multiMatchQuery("Spring微服务实战", "name", "description")
                        .minimumShouldMatch("70%")
                        .field("name", 10);
        searchSourceBuilder.query(multiMatchQueryBuilder);
        // 给搜索请求对象设置搜索源
        searchRequest.source(searchSourceBuilder);
        // 发起请求，获取结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 打印结果集
        printResult(searchHits);
    }

    /**
     * 布尔查询
     * @throws IOException
     */
    @Test
    public void testBoolSearch() throws IOException {
        // 基础设置
        SearchRequest searchRequest = new SearchRequest("bigsea_demo");
        // 搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // bool搜索
        // 第一个query
        MultiMatchQueryBuilder multiMatchQueryBuilder =
                QueryBuilders.multiMatchQuery("Spring微服务实战", "name", "description")
                        .minimumShouldMatch("70%")
                        .field("name", 10);
        // 第二个query
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("price", 86.5);

        // 定义一个布尔query，将上面两个条件组合在一起
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        // 给搜索请求对象设置搜索源
        searchRequest.source(searchSourceBuilder);
        // 发起请求，获取结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 打印结果集
        printResult(searchHits);
    }

    /**
     * 过滤器（对于搜索结果的过滤，效率高，推荐) + 排序
     * @throws IOException
     */
    @Test
    public void testFilter() throws IOException {
        // 基础设置
        SearchRequest searchRequest = new SearchRequest("bigsea_demo");
        // 搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // bool搜索
        // 第一个query
        MultiMatchQueryBuilder multiMatchQueryBuilder =
                QueryBuilders.multiMatchQuery("Spring微服务实战", "name", "description")
                        .minimumShouldMatch("70%")
                        .field("name", 10);
        // 第二个query
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("price", 86.5);
        // 定义一个布尔query，将上面两个条件组合在一起
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);
        // 过滤器
        boolQueryBuilder.filter(QueryBuilders.termQuery("price", 86.6));
        // 设置
        searchSourceBuilder.query(boolQueryBuilder);
        // 定义排序
        searchSourceBuilder.sort(new FieldSortBuilder("price").order(SortOrder.ASC));

        // 给搜索请求对象设置搜索源
        searchRequest.source(searchSourceBuilder);
        // 发起请求，获取结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 打印结果集
        printResult(searchHits);
    }
}
