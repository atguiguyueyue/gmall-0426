package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    //查询总数数据抽象方法
    public Double selectOrderAmountTotal(String date);

    //查询分时数据抽象方法
    public List<Map> selectOrderAmountHourMap(String date);

}
