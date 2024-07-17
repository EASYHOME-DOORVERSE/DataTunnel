package com.dongwo.data.tunnel.canal.factory;

import com.dongwo.data.tunnel.canal.handler.AbstractGeneralEntryHandler;
import com.dongwo.data.tunnel.canal.handler.EntryHandler;
import com.dongwo.data.tunnel.canal.handler.flatmessage.AbstractGeneralFlatMessageEntryHandler;
import com.dongwo.data.tunnel.canal.handler.flatmessage.FlatMessageEntryHandler;
import com.dongwo.data.tunnel.canal.utils.HandlerUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 5:27 PM
 */
@Slf4j
@Component
public class HandlerFactory implements InitializingBean {

    /**
     * 非扁平消息处理器集合
     */
    @Autowired(required = false)
    private List<AbstractGeneralEntryHandler> entryHandlers;

    /**
     * 扁平消息处理器集合
     */
    @Autowired(required = false)
    private List<AbstractGeneralFlatMessageEntryHandler> flatMessageEntryHandlers;


    private Map<String, List<EntryHandler>> handlerMap = new HashMap<>();
    private Map<String, List<FlatMessageEntryHandler>> flatMessageHandlerMap = new HashMap<>();


    @Override
    public void afterPropertiesSet() throws Exception {
        if(CollectionUtils.isNotEmpty(entryHandlers)){
            for (AbstractGeneralEntryHandler entryHandler : entryHandlers) {
                String handlerKey = HandlerUtils.getHandlerKey(entryHandler);
                handlerMap.computeIfAbsent(handlerKey, k -> new ArrayList<>()).add(entryHandler);
            }
        }

        if(CollectionUtils.isNotEmpty(flatMessageEntryHandlers)){
            for (AbstractGeneralFlatMessageEntryHandler flatMessageEntryHandler : flatMessageEntryHandlers) {
                String handlerKey = HandlerUtils.getHandlerKey(flatMessageEntryHandler);
                flatMessageHandlerMap.computeIfAbsent(handlerKey, k -> new ArrayList<>()).add(flatMessageEntryHandler);
            }
        }
    }


    /**
     * 获取handler
     * @param database
     * @param table
     * @return
     */
    public List<EntryHandler> getHandlers(String database, String table) {
        List<EntryHandler> handlers = Lists.newArrayList();

        String generalHandlerKey = HandlerUtils.getHandlerKey("*", "*");
        List<EntryHandler> generalHandlers = handlerMap.getOrDefault(generalHandlerKey, Collections.emptyList());

        String handlerKey = HandlerUtils.getHandlerKey(database, table);
        List<EntryHandler> bizHandlers = handlerMap.getOrDefault(handlerKey, Collections.emptyList());

        handlers.addAll(generalHandlers);
        handlers.addAll(bizHandlers);
        return handlers;
    }

    /**
     *
     * @param database
     * @param table
     * @return
     */
    public List<FlatMessageEntryHandler> getFlatMessageHandlers(String database, String table) {
        List<FlatMessageEntryHandler> handlers = Lists.newArrayList();

        String generalHandlerKey = HandlerUtils.getHandlerKey("*", "*");
        List<FlatMessageEntryHandler> generalHandlers = flatMessageHandlerMap.getOrDefault(generalHandlerKey, Collections.emptyList());

        String handlerKey = HandlerUtils.getHandlerKey(database, table);
        List<FlatMessageEntryHandler> bizHandlers = flatMessageHandlerMap.getOrDefault(handlerKey, Collections.emptyList());

        handlers.addAll(generalHandlers);
        handlers.addAll(bizHandlers);
        return handlers;
    }
}
