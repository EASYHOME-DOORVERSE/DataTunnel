package com.dongwo.data.tunnel.canal.handler.flatmessage;

import java.util.List;
import java.util.Map;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 4:57 PM
 */
public interface FlatMessageEntryHandler {

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
     * 处理 新增
     *
     * @param data
     */
    void handleInsert(List<Map<String, String>> data) throws Exception;

    /**
     * 处理 更新
     *  @param old
     * @param data
     */
    void handleUpdate(List<Map<String, String>> old, List<Map<String, String>> data) throws Exception;

    /**
     * 处理 删除
     *
     * @param data
     */
    void handleDelete(List<Map<String, String>> data) throws Exception;

}
