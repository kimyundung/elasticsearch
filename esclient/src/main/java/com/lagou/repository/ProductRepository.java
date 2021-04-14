package com.lagou.repository;

import com.lagou.pojo.Product;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * 当SDE访问索引库时,需要定义一个持久层的接口去继承ElasticsearchRepository接口, 无需实现
 */
public interface ProductRepository extends ElasticsearchRepository<Product,Long> {

    /**
     * 按价格范围查询
     * @param from
     * @param to
     * @return
     */
    List<Product> findByPriceBetween(Double from,Double to);
}
