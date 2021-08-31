package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //处理日活总数抽象方法
    public Integer getDauTotal(String date);

    //处理日活分时抽象方法
    public Map getDauTotalHour(String date);

    //处理交易额总数抽象方法
    public Double getGmvTotal(String date);

    //处理交易额分时抽象方法
    public Map getGmvTotalHour(String date);


}
