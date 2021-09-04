package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String selectDauTotal(@RequestParam("date") String date){
        //1.获取service层处理后的数据
        Integer dauTotal = publisherService.getDauTotal(date);

        Double gmvTotal = publisherService.getGmvTotal(date);

        //2.创建map集合用来存放新增日活数据
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //3.创建map集合用来存放新增设备数据
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        //创建map集合用来存放新增交易额数据
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", gmvTotal);

        //4.创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        //5.将list集合转为json字符串返回
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String selectDauTotalHour(@RequestParam("id") String id, @RequestParam("date") String date) {

        //1.创建Map集合用来存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //2.获取service中的数据
        //2.1获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;
        Map yesterdayMap = null;

        //通过id来判断返回的应该是谁的数据
        if ("dau".equals(id)){
            //获取今天的分时数据
            todayMap  = publisherService.getDauTotalHour(date);
            //3.获取昨天的分时数据
            yesterdayMap = publisherService.getDauTotalHour(yesterday);
        }else if ("order_amount".equals(id)){
            //获取交易额的分时数据
            todayMap = publisherService.getGmvTotalHour(date);
            yesterdayMap = publisherService.getGmvTotalHour(yesterday);
        }


        //4.将数据封装到新的map集合中
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") Integer startpage,
                                @RequestParam("size") Integer size,
                                @RequestParam("keyword") String keyword) throws IOException {
        //获取Service层封装好的数据
        Map map = publisherService.SaleDetail(date, startpage, size, keyword);
        return JSONObject.toJSONString(map);
    }
}
