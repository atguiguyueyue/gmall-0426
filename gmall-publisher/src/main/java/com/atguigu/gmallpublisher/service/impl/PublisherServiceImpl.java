package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHour(String date) {
        //1.获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //1.2创建map集合用来存放新的数据
        HashMap<String, Long> result = new HashMap<>();

        //2.遍历list集合
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getGmvTotalHour(String date) {
        //1.获取数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建map集合用来存放新的数据
        HashMap<String, Double> result = new HashMap<>();

        //3.遍历list集合获取到里面每个原始的map
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map SaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_DETAIL + "0426").addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        //TODO 1.命中数据条数
        Long total = searchResult.getTotal();

        //TODO 2.获取明细数据
        //创建list集合用来存放明细数据
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            for (Object o : hit.source.keySet()) {
                System.out.println(o+":"+hit.source.get(o));
            }
            detail.add(hit.source);
        }

        //TODO 3.获取聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();

        //TODO 年龄
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets = groupby_user_age.getBuckets();
        //20岁以下的个数
        int low20Count = 0;
        //30岁及30岁以上的个数
        int up30Count = 0;
        for (TermsAggregation.Entry bucket : buckets) {
            if (Integer.parseInt(bucket.getKey()) < 20) {
                low20Count += bucket.getCount();
            }else if(Integer.parseInt(bucket.getKey())>=30){
                up30Count += bucket.getCount();
            }
        }

        //20岁以下的占比
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;

        //30岁以上的占比
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;

        //20岁到30岁占比
        double up20Low30Ratio = Math.round((100D - low20Ratio - up30Ratio) * 10D) / 10D;

        //创建option对象用来存放年龄占比的具体数据
        Option low20Opt = new Option("20岁以下", low20Ratio);
        Option up20Low30Opt = new Option("20岁到30岁", up20Low30Ratio);
        Option up30Opt = new Option("30岁及30岁以上", up30Ratio);

        //创建存放年龄占比的list集合
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(low20Opt);
        ageOptions.add(up20Low30Opt);
        ageOptions.add(up30Opt);

        //创建年龄占比的Stat对象
        Stat ageStat = new Stat(ageOptions, "用户年龄占比");

        //TODO 性别
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> genderBucks = groupby_user_gender.getBuckets();
        //男生个数
        int maleCount = 0;
        for (TermsAggregation.Entry genderBuck : genderBucks) {
            if ("M".equals(genderBuck.getKey())){
                maleCount += genderBuck.getCount();
            }
        }
        //男生占比
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        //女生占比
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        //创建option对象存放性别占比的具体数据
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        //创建性别占比的list集合
        ArrayList<Option> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        //创建性别占比的Stat对象
        Stat genderStat = new Stat(genderList, "用户性别占比");

        //创建存放Stat对象的集合
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //创建Map集合用来存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        return result;
    }
}
