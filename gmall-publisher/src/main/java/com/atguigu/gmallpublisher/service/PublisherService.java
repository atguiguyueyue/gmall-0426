package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //处理日活总数抽象方法
    public Integer getDauTotal(String date);

    public Map getDauTotalHour(String date);
}
