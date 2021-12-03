package com.myx.gamll.publisher.service;

/**
 * @Description: ES相关操作接口
 * @author: mayx
 * @date: 2021/12/3 15:34
 */
public interface ESService {
    // 日活的总数查询
    public Long getDauTotal(String date);
}
