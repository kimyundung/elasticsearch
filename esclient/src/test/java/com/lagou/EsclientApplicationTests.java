package com.lagou;


import com.google.gson.Gson;
import com.lagou.pojo.Product;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
class EsclientApplicationTests {

    /**
     * gson对象
     */
    private Gson gson = new Gson();
    /**
     * 客户端
     */
    private RestHighLevelClient restHighLevelClient;

    /**
     * 初始化客户端
     */
    @BeforeEach
    public void init() {
        System.out.println("--------------- init");
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("127.0.0.1",9201,"http"),
                new HttpHost("127.0.0.1",9202,"http"),
                new HttpHost("127.0.0.1",9203,"http"));
        restHighLevelClient = new RestHighLevelClient(restClientBuilder);
    }

    @Test
    public void test(){
        System.out.println(restHighLevelClient);
    }

    /**
     * 插入文档
     */
    @Test
    public void testInsert() throws IOException {
        //1 文档数据
        Product product = new Product();
        product.setId(6L);
        product.setTitle("华为META10");
        product.setCategory("手机");
        product.setBrand("华为");
        product.setPrice(4499D);
        product.setImages("https://p6-tt-ipv6.byteimg.com/origin/pgc-image/5205160d8a60417393cfea82c1550140");

        //2 将文档数据->json (gson)
        String json = gson.toJson(product);

        //3 创建索引请求对象 访问那个索引库, type, 文档id
        //  IndexRequest(String index, String type, String id)
        IndexRequest request = new IndexRequest("lagou","item",product.getId().toString());
        request.source(json, XContentType.JSON);

        //4 发出请求
        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    /**
     * 查询文档
     */
    @Test
    public void testQuery() throws IOException {
        //1 初始化GetRequest对象
        //  GetRequest(String index, String type, String id)
        GetRequest getRequest = new GetRequest("lagou","item","1");

        //2 执行查询
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);

        //3 取数据
        String source = getResponse.getSourceAsString();
        Product product = gson.fromJson(source, Product.class);
        System.out.println(product);
    }

    /**
     * 删除文档
     */
    @Test
    public void testDelete() throws IOException {
        //1 初始化DeleteRequest对象
        DeleteRequest deleteRequest = new DeleteRequest("lagou","item","1");
        //2 执行删除
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse);
    }

    /**
     * 查询所有
     */
    @Test
    public void testMatchAll() throws IOException {
        //2 查询工具
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //3 添加查询条件, 执行查询类型
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        // 调用自定义方法
        baseQuery(sourceBuilder);
    }

    /**
     * 关键字搜索
     */
    @Test
    public void testMatchQuery() throws IOException {
        //2 查询工具
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //3 添加查询条件, 执行查询类型
        sourceBuilder.query(QueryBuilders.matchQuery("title","手机"));
        // 调用自定义方法
        baseQuery(sourceBuilder);
    }

    /**
     * 范围查询
     */
    @Test
    public void testRangeQuery() throws IOException {
        //2 查询工具
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //3 添加查询条件, 执行查询类型
        sourceBuilder.query(QueryBuilders.rangeQuery("price").gte(2000).lte(4000));
        // 调用自定义方法
        baseQuery(sourceBuilder);
    }

    /**
     * 过滤 (fetchSource)
     */
    @Test
    public void testSourceFilter() throws IOException {
        //2 查询工具
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //3 添加查询条件, 执行查询类型
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //4 过滤
        sourceBuilder.fetchSource(new String[]{"id","title","price"},null);
        // 调用自定义方法
        baseQuery(sourceBuilder);
    }

    /**
     * 排序
     */
    @Test
    public void testSort() throws IOException {
        //2 查询工具
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //3 添加查询条件, 执行查询类型
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //4 过滤
        sourceBuilder.fetchSource(new String[]{"id","title","price"},null);
        //4 排序
        sourceBuilder.sort("price", SortOrder.DESC);
        // 调用自定义方法
        baseQuery(sourceBuilder);
    }

    /**
     * 分页
     */
    @Test
    public void testPage() throws IOException {
        //2 查询工具
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //3 添加查询条件, 执行查询类型
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //4 过滤
        sourceBuilder.fetchSource(new String[]{"id","title","price"},null);
        //4 排序
        sourceBuilder.sort("price", SortOrder.DESC);
        //5 分页
        int num = 2;
        int size = 3;
        int from = (num-1)*size;
        sourceBuilder.from(from);
        sourceBuilder.size(size);
        // 调用自定义方法
        baseQuery(sourceBuilder);
    }

    public void baseQuery(SearchSourceBuilder sourceBuilder) throws IOException {
        //1 搜索请求对象
        SearchRequest searchRequest = new SearchRequest();

        searchRequest.source(sourceBuilder);
        //4 执行查询
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //5 获取查询结果
        SearchHits hits = searchResponse.getHits();
        //6 获取文档数组
        SearchHit[] hitsHits = hits.getHits();
        for(SearchHit searchHit: hitsHits){
            String json = searchHit.getSourceAsString();
            Product product = gson.fromJson(json, Product.class);
            System.out.println(product);
        }
    }

    /**
     * 关闭客户端
     */
    @AfterEach
    public void close() throws IOException {
        restHighLevelClient.close();
        System.out.println("--------------- close");
    }

}
