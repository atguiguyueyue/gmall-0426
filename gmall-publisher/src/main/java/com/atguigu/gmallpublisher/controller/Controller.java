package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

        //4.创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();
        result.add(dauMap);
        result.add(devMap);

        //5.将list集合转为json字符串返回
        return JSONObject.toJSONString(result);
    }
}
