package com.atguigu.gmallpublisher.mapper;

public interface DauMapper {
    //查询日活总数的抽象方法
    public Integer selectDauTotal(String date);
}
