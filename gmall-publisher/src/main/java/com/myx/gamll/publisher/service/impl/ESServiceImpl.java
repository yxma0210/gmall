package com.myx.gamll.publisher.service.impl;

import com.myx.gamll.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description: ES相关操作接口实现类
 * @author: mayx
 * @date: 2021/12/3 15:35
 */
@Service
public class ESServiceImpl implements ESService {

    //将ES的客户端操作对象注入到Service中
    @Autowired
    JestClient jestClient;

    /*
    GET gmall_dau_info_2021-12-01-query/_search
    {
      "query": {
        "match_all": {}
      }
    }
     */
    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();
        String indexName = "gmall_dau_info_"+date+"-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        Long total = 0L;
        try {
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败");
        }
        return total;
    }

    @Override
    public Map getDauHour(String date) {
        String indexName = "gmall_dau_info_" + date + "-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms(
                "groupby").field("hr").size(24);
        searchSourceBuilder.aggregation(aggBuilder);
        String query = searchSourceBuilder.toString();
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .addType("_doc")
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            MetricAggregation aggregations = result.getAggregations();
            // 定义集合，封装返回结果
            HashMap<String, Long> aggMap = new HashMap<>();
            if (aggregations.getTermsAggregation("groupby") != null) {
                List<TermsAggregation.Entry> buckets = aggregations.getTermsAggregation("groupby").getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    aggMap.put(bucket.getKey(), bucket.getCount());
                }
            }
            return aggMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败");
        }
    }
}
