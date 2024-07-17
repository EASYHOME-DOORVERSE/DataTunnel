package com.dongwo.data.tunnel.canal.utils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.dongwo.data.tunnel.canal.enums.HandlerTypeEnum;
import com.dongwo.data.tunnel.canal.handler.EntryHandler;
import com.dongwo.data.tunnel.canal.handler.flatmessage.FlatMessageEntryHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 5:34 PM
 */
public class HandlerUtils {

    public static String getHandlerKey(FlatMessageEntryHandler entryHandler) {
        return getHandlerKey(entryHandler.database(), entryHandler.table());
    }

    public static String getHandlerKey(String database, String table) {
        return StringUtils.join(Arrays.asList(database, table), ".");
    }

    public static String getHandlerKey(EntryHandler entryHandler) {
        return getHandlerKey(entryHandler.database(), entryHandler.table());
    }


    public static HandlerTypeEnum transform(CanalEntry.EventType eventType) {
        switch (eventType) {
            case INSERT:
                return HandlerTypeEnum.INSERT;
            case UPDATE:
                return HandlerTypeEnum.UPDATE;
            case DELETE:
                return HandlerTypeEnum.DELETE;
            default:
                return null;
        }
    }

    public static HandlerTypeEnum transform(String eventType) {
        switch (eventType) {
            case "INSERT":
                return HandlerTypeEnum.INSERT;
            case "UPDATE":
                return HandlerTypeEnum.UPDATE;
            case "DELETE":
                return HandlerTypeEnum.DELETE;
            default:
                return null;
        }
    }
}
