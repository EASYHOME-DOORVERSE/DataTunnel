package com.dongwo.data.tunnel.canal.handler;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.Editor;
import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 4:57 PM
 */
@Slf4j
public abstract class AbstractGeneralEntryHandler implements EntryHandler {

    /**
     * 获取id
     * 处理器标识，非唯一性。
     * 用来识别处理器可以处理的库.表.列
     *
     * @return
     */
    protected abstract String getId();

    /**
     * 获取库名
     *
     * @return
     */
    @Override
    public final String database() {
        return getId().split("\\.")[0];
    }

    /**
     * 获取表名
     *
     * @return
     */
    @Override
    public final String table() {
        return getId().split("\\.")[1];
    }

    /**
     * 获取字段
     *
     * @return
     */
    @Override
    public final String columns() {
        String[] split = getId().split("\\.");
        if (split.length != 3) {
            return "";
        }
        return split[2];
    }

    /**
     * 具体处理器路由函数
     * 可扩展前置后置处理器
     *
     * @param eventType
     * @param header
     * @param beforeColumnsList
     * @param afterColumnsList
     */
    @Override
    public final void handle(CanalEntry.EventType eventType, CanalEntry.Header header, List<CanalEntry.Column> beforeColumnsList, List<CanalEntry.Column> afterColumnsList) throws Exception {
        switch (eventType){
            case INSERT:
                this.handleInsert(header, afterColumnsList);
                break;
            case UPDATE:
                boolean predicateCondition = this.predicateCondition(afterColumnsList);
                if(!predicateCondition){
                    return;
                }
                this.handleUpdate(header, beforeColumnsList, afterColumnsList);
                break;
            case DELETE:
                this.handleDelete(header, beforeColumnsList);
                break;
            default:
                throw new RuntimeException("不支持的eventType:" + eventType);
        }
    }

    /**
     * 处理 新增
     *
     * @param header
     * @param afterColumns
     */
    protected void handleInsert(CanalEntry.Header header, List<CanalEntry.Column> afterColumns) throws Exception{

    }

    /**
     * 处理 更新
     *
     * @param header
     * @param beforeColumns
     * @param afterColumns
     */
    protected void handleUpdate(CanalEntry.Header header, List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns) throws Exception{

    }

    /**
     * 处理 删除
     *
     * @param header
     * @param beforeColumns
     */
    protected void handleDelete(CanalEntry.Header header, List<CanalEntry.Column> beforeColumns) throws Exception{

    }



    /**
     * 将columnList 转换为 clazz
     *
     * @param columnList
     * @param clazz
     * @param <T>
     * @return
     */
    protected final  <T> T convertToCamelCase(List<CanalEntry.Column> columnList, Class<T> clazz) {
        Map<String, Object> columnMap = columnList.stream().collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
        Object bean = BeanUtil.toBean(columnMap, clazz);
        return (T) bean;
    }

    protected final  <T> T convertToCamelCase(List<CanalEntry.Column> columnList, Class<T> clazz, Map<String, String> tsMap) {
        Map<String, Object> columnMap = columnList.stream().collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
        Object bean = BeanUtil.toBean(columnMap, clazz, CopyOptions.create().setFieldNameEditor(column -> tsMap.getOrDefault(column, column)));
        return (T) bean;
    }



    /**
     * 断言条件
     *
     * @param afterColumns
     * @return
     */
    private boolean predicateCondition(List<CanalEntry.Column> afterColumns) {
        String columnsRegex = columns();
        // 全
        if(Objects.equals(columnsRegex, "*")){
            return true;
        }
        // 或
        Set<String> updatedColumns = afterColumns.stream().filter(CanalEntry.Column::getUpdated).map(CanalEntry.Column::getName).collect(Collectors.toSet());
        return Arrays.stream(columnsRegex.split("\\|")).map(String::trim).anyMatch(updatedColumns::contains);
    }


}
