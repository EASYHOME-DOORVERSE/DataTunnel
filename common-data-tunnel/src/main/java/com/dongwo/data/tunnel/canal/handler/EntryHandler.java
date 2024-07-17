package com.dongwo.data.tunnel.canal.handler;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 4:57 PM
 */
public interface EntryHandler {

    /**
     * 处理的库
     *
     * @return
     */
    String database();

    /**
     * 处理的表
     *
     * @return
     */
    String table();

    /**
     * 处理的列
     *
     * @return
     */
    String columns();


    /**
     * 处理 入口
     * 模版方法总处理函数
     *
     * @param eventType
     * @param header
     * @param beforeColumnsList
     * @param afterColumnsList
     */
    void handle(CanalEntry.EventType eventType, CanalEntry.Header header, List<CanalEntry.Column> beforeColumnsList, List<CanalEntry.Column> afterColumnsList) throws Exception;


}
