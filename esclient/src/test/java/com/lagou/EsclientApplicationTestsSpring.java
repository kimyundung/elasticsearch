package com.lagou;


import com.lagou.pojo.Product;
import com.lagou.repository.ProductRepository;
import com.lagou.resultmapper.ESSearchResultMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
class EsclientApplicationTestsSpring {

    @Autowired
    private ElasticsearchTemplate template;
    @Autowired
    private ProductRepository productRepository;

    @Test
    public void check(){
        System.out.println("--------------" + template);
    }

    /**
     * 创建索引库
     */
    @Test
    public void createIndex(){
        // 创建索引
        template.createIndex(Product.class);
    }

    /**
     * 创建类型映射
     */
    @Test
    public void creatType(){
        template.putMapping(Product.class);
    }

    /**
     * 插入一个文档
     */
    @Test
    public void insertDocument(){
        Product product = new Product(1L,"锤子手机","手机","锤子",3299.99,"https://p6-tt-ipv6.byteimg.com/origin/pgc-image/5205160d8a60417393cfea82c1550140");
        productRepository.save(product);
        System.out.println(">>>>>>>>> success");
    }

    /**
     * 插入一堆文档
     */
    @Test
    public void insertDocuments(){
        Product product1 = new Product(2L,"坚果手机","手机","坚果",3399.99,"https://p6-tt-ipv6.byteimg.com/origin/pgc-image/5205160d8a60417393cfea82c1550140");
        Product product2 = new Product(3L,"华为手机","手机","华为",3399.99,"https://p6-tt-ipv6.byteimg.com/origin/pgc-image/5205160d8a60417393cfea82c1550140");
        Product product3 = new Product(4L,"苹果手机","手机","苹果",3399.99,"https://p6-tt-ipv6.byteimg.com/origin/pgc-image/5205160d8a60417393cfea82c1550140");
        Product product4 = new Product(5L,"索尼手机","手机","索尼",3399.99,"https://p6-tt-ipv6.byteimg.com/origin/pgc-image/5205160d8a60417393cfea82c1550140");
        Product product5 = new Product(6L,"小米手机","手机","小米",3399.99,"https://p6-tt-ipv6.byteimg.com/origin/pgc-image/5205160d8a60417393cfea82c1550140");

        List<Product> list = new ArrayList<>();
        list.add(product1);
        list.add(product2);
        list.add(product3);
        list.add(product4);
        list.add(product5);
        productRepository.saveAll(list);
        System.out.println(">>>>>>>>> success");
    }

    /**
     * 根据id查询
     */
    @Test
    public void findById(){
        Optional<Product> optional = productRepository.findById(3l);
        //取出数据
        //  orElse方法的作用: 如果optional中封装的实体对象为空, 也就是没有从索引库中查询出匹配的文档, 返回null
        Product product = optional.orElse(null);
        System.out.println(">>>>>>>>>> "+product);
    }

    /**
     * 根据id查询 (兜底)
     */
    @Test
    public void findById2(){
        Optional<Product> optional = productRepository.findById(100l);
        //取出数据
        //  orElse方法的作用: 如果optional中封装的实体对象为空, 也就是没有从索引库中查询出匹配的文档, 返回null
        Product defaultProduct = new Product();
        defaultProduct.setTitle("default data");
        Product product = optional.orElse(defaultProduct);
        System.out.println(">>>>>>>>>> "+product);
    }

    /**
     * 查询所有
     */
    @Test
    public void findAll(){
        Iterable<Product> iterable = productRepository.findAll();
        iterable.forEach(System.out::println);
    }

    /**
     * 按价格范围查询
     */
    @Test
    public void findByPriceBetween(){
        List<Product> list = productRepository.findByPriceBetween(2000D, 4000D);
        System.out.println(list);
    }

    /**
     * 原生查询
     *
     * 需求：
     * 查询title中包含<小米手机>的商品，
     * 以价格升序排序，
     * 分页查询：每页展示2条，查询第1页。
     * 对查询结果进行聚合分析：获取品牌及个数
     */
    @Test
    public void nativeQuery(){
        //1 构建原生查询器
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //2 source过滤
        //  FetchSourceFilter(String[] includes, String[] excludes)    new String[]{"x","x"}
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[0],new String[0]));
        //3 查询条件
        queryBuilder.withQuery(QueryBuilders.matchQuery("title","小米手机"));
        //4 设置分页+排序
        queryBuilder.withPageable(PageRequest.of(0,3,Sort.by(Sort.Direction.DESC,"price")));
        //5 聚合
        queryBuilder.addAggregation(AggregationBuilders.terms("brandAgg").field("brand"));
        //7 查询
        AggregatedPage<Product> results = template.queryForPage(queryBuilder.build(), Product.class);
        //6 总数
        long total = results.getTotalElements();
        //8 总页数
        int totalPages = results.getTotalPages();
        //本页数据集合
        List<Product> content = results.getContent();
        System.out.println(total);
        System.out.println(totalPages);
        content.stream().forEach(product -> System.out.println(product));

        //获得聚合的结果
        Aggregations aggregations = results.getAggregations();
        Terms terms = aggregations.get("brandAgg");
        //获取桶, 遍历桶中的内容
        terms.getBuckets().forEach(b->{
            System.out.println("品牌:" + b.getKeyAsString()+ "\t"+b.getDocCount());

        });
    }


    /**
     * 原生查询 + 高亮
     *
     * 需求：
     * 查询title中包含<小米手机>的商品，
     * 以价格升序排序，
     * 分页查询：每页展示2条，查询第1页。
     * 对查询结果进行聚合分析：获取品牌及个数
     */
    @Test
    public void nativeQueryHighLight(){
        //1 构建原生查询器
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //2 source过滤
        //  FetchSourceFilter(String[] includes, String[] excludes)    new String[]{"x","x"}
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[0],new String[0]));
        //3 查询条件
        queryBuilder.withQuery(QueryBuilders.matchQuery("title","小米手机"));
        //4 设置分页+排序
        queryBuilder.withPageable(PageRequest.of(0,6,Sort.by(Sort.Direction.DESC,"price")));
        //0 高亮
        HighlightBuilder.Field field = new HighlightBuilder.Field("title");
        field.preTags("<font style='color:red'>");
        field.postTags("</font>");
        queryBuilder.withHighlightFields(field);
        //5 聚合
        queryBuilder.addAggregation(AggregationBuilders.terms("brandAgg").field("brand"));
        //7 查询
        AggregatedPage<Product> results = template.queryForPage(queryBuilder.build(), Product.class, new ESSearchResultMapper());
        //6 总数
        long total = results.getTotalElements();
        //8 总页数
        int totalPages = results.getTotalPages();
        //本页数据集合
        List<Product> content = results.getContent();
        System.out.println(total);
        System.out.println(totalPages);
        content.stream().forEach(product -> System.out.println(product));

    }
}
