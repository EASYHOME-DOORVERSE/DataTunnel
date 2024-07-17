package com.dongwo.data.tunnel.canal.handler.flatmessage;

import java.util.List;
import java.util.Map;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 4:57 PM
 */
public abstract class AbstractGeneralFlatMessageEntryHandler implements FlatMessageEntryHandler{

    /**
     * 获取id
     *
     * @return
     */
    protected abstract String getId();

    @Override
    public String database() {
        return getId().split("\\.")[0];
    }

    @Override
    public String table() {
        return getId().split("\\.")[1];
    }

    @Override
    public String columns() {
        String[] split = getId().split("\\.");
        if (split.length != 3) {
            return "";
        }
        return split[2];
    }

    @Override
    public void handleInsert(List<Map<String, String>> afterColumns) throws Exception {

    }

    @Override
    public void handleUpdate(List<Map<String, String>> beforeColumns, List<Map<String, String>> afterColumns) throws Exception {

    }

    @Override
    public void handleDelete(List<Map<String, String>> beforeColumns) throws Exception {

    }

}
