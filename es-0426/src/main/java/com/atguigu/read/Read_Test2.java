package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Read_Test2 {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.查询es中的数据
        //TODO 相当于最外成花括号
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //TODO ---------------------------bool-----------------------------------
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //TODO ---------------------------term-----------------------------------
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "男");

        //TODO ---------------------------filter-----------------------------------
        boolQueryBuilder.filter(termQueryBuilder);

        //TODO ---------------------------query-----------------------------------
        searchSourceBuilder.query(boolQueryBuilder);

        //TODO ---------------------------match-----------------------------------
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "羽毛球");

        //TODO ---------------------------must-----------------------------------
        boolQueryBuilder.must(matchQueryBuilder);

        //TODO ---------------------------aggs-----------------------------------
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByClass").field("class_id");
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByAge").field("age");
        searchSourceBuilder.aggregation(termsAggregationBuilder.subAggregation(maxAggregationBuilder));

        //TODO ---------------------------from-----------------------------------
        searchSourceBuilder.from(0);

        //TODO ---------------------------size-----------------------------------
        searchSourceBuilder.size(2);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();
        SearchResult result = jestClient.execute(search);

        //5.获取命中条数
        System.out.println("命中："+result.getTotal()+"条数据");

        //6.获取数据明细相关的内容
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            //获取索引名
            System.out.println("_index:"+hit.index);
            //获取类型
            System.out.println("_type:"+hit.type);
            //获取文档id
            System.out.println("_id:"+hit.id);

            //获取数据明细
            Map map = hit.source;
            for (Object o : map.keySet()) {
                System.out.println(o+":"+map.get(o));
            }
        }
        //7.获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:"+bucket.getKey());
            System.out.println("doc_count:"+bucket.getCount());

            //8.获取嵌套聚合中最大年龄的聚合组数据
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value:"+groupByAge.getMax());
        }


        //关闭连接
        jestClient.shutdownClient();
    }
}
