package com.atguigu.gmallpublisher.service;

import java.io.IOException;
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

    //处理灵活分析数据的抽象方法
    public Map SaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException;


}
