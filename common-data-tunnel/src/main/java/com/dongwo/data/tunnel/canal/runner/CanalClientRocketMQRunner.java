package com.dongwo.data.tunnel.canal.runner;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.dongwo.data.tunnel.canal.config.CanalTunnelConfig;
import com.dongwo.data.tunnel.canal.enums.HandlerTypeEnum;
import com.dongwo.data.tunnel.canal.factory.HandlerFactory;
import com.dongwo.data.tunnel.canal.handler.EntryHandler;
import com.dongwo.data.tunnel.canal.utils.CanalNamedThreadFactory;
import com.dongwo.data.tunnel.canal.utils.HandlerUtils;
import com.dongwo.data.tunnel.manager.CanalTunnelFailureRecordManager;
import com.dongwo.data.tunnel.po.CanalTunnelFailureRecordPO;
import com.easyhome.common.utils.SnowflakeIdWorker;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 2:59 PM
 */
@Slf4j
@Component
@ConditionalOnMissingBean(value = CanalClientRocketMQFlatMessageRunner.class)
public class CanalClientRocketMQRunner implements ApplicationRunner, DisposableBean {

    /**
     * canal 配置
     */
    @Resource
    private CanalTunnelConfig canalTunnelConfig;

    /**
     * 处理器工厂
     */
    @Resource
    private HandlerFactory handlerFactory;

    /**
     * 同步失败回调管理器
     */
    @Resource
    private CanalTunnelFailureRecordManager canalTunnelFailureRecordManager;

    /**
     * 启动成功
     */
    private volatile boolean running = false;

    /**
     * 线程池
     */
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new CanalNamedThreadFactory("CanalConnector-withoutAck-message"));

    /**
     * canal rocketMq 连接器
     */
    private RocketMQCanalConnector connector;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Canal Client RocketMQ Runner 开始启动");
        CanalTunnelConfig.RocketMq rocketMQConfig = canalTunnelConfig.getRocketMq();
        log.info("Canal Client RocketMQ Runner 配置信息 : {}", JSON.toJSONString(rocketMQConfig));
        // 创建链接
        connector = new RocketMQCanalConnector(
                rocketMQConfig.getNameServers(),
                rocketMQConfig.getTopic(), rocketMQConfig.getConsumer().getGroupId(),
                rocketMQConfig.getAccessKey(), rocketMQConfig.getSecretKey(),
                -1, false,
                rocketMQConfig.getEnableMessageTrace(), rocketMQConfig.getCustomizedTraceTopic(),
                rocketMQConfig.getAccessChannel(), rocketMQConfig.getNamespace());
        connector.connect();
        connector.subscribe();
        connector.rollback();
        running = true;
        log.info("Canal Client RocketMQ Runner 开始调度");
        executor.scheduleAtFixedRate(() -> {
            try {
                if (!running) {
                    log.info("Canal Client RocketMQ Runner 停止调度");
                    return;
                }
                List<Message> messages = connector.getListWithoutAck(200L, TimeUnit.MILLISECONDS);// 获取message
                for (Message message : messages) {
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId != -1 && size != 0) {
                        this.printEntry(message.getEntries());
                    }
                }
                connector.ack();
            } catch (Throwable e) {
                log.info("Canal Client RocketMQ Runner 防止异常未捕获导致任务停止. Error : {}", e.getMessage(), e);
            }
        }, 200, 200, TimeUnit.MILLISECONDS);
        log.info("Canal Client RocketMQ Runner 完成调度");
    }


    /**
     * 批量打印条目
     *
     * @param entrys
     */
    private void printEntry(List<CanalEntry.Entry> entrys) {
        try {
            for (CanalEntry.Entry entry : entrys) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    continue;
                }

                CanalEntry.RowChange rowChange;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Throwable e) {
                    log.info("Canal Client RocketMQ Runner CanalEntry 解析异常 : {}", entry,  e);
                    continue;
                }

                CanalEntry.EventType eventType = rowChange.getEventType();

                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    List<EntryHandler> entryHandlers = handlerFactory.getHandlers(entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                    for (EntryHandler entryHandler : entryHandlers) {
                        try {
                            entryHandler.handle(eventType,entry.getHeader(), rowData.getBeforeColumnsList(), rowData.getAfterColumnsList());
                        } catch (Throwable e) {
                            log.info("Canal Client RocketMQ Runner CanalEntry 处理异常 data : {}, EntryHandler : {}", entry, entryHandler.getClass().getName(), e);
                            CanalEntryHeader canalEntryHeader = this.getCanalEntryHeader(entry.getHeader());
                            List<CanalEntryColumn> beforeCanalEntryColumn = this.getCanalEntryColumnList(rowData.getBeforeColumnsList());
                            List<CanalEntryColumn> afterCanalEntryColumn = this.getCanalEntryColumnList(rowData.getAfterColumnsList());
                            CanalTunnelFailureRecordPO recordPO = new CanalTunnelFailureRecordPO().setRecordId(SnowflakeIdWorker.generateId()).setSchemaName(entry.getHeader().getSchemaName()).setTableName(entry.getHeader().getTableName()).setHandlerType(HandlerUtils.transform(eventType)).setHandlerClazz(entryHandler.getClass().getName()).setEntry(JSON.toJSONString(canalEntryHeader))
                                    .setBeforeColumnsList(JSON.toJSONString(beforeCanalEntryColumn)).setAfterColumnsList(JSON.toJSONString(afterCanalEntryColumn));

                            canalTunnelFailureRecordManager.record(recordPO);
                        }
                    }
                }
            }
        }catch (Exception e){
            log.info("Canal Client RocketMQ Runner CanalEntry 打印异常 Error : {}", e.getMessage(), e);
        }
    }

    /**
     * 获取CanalEntryHeader
     *
     * @param header
     * @return
     */
    private CanalEntryHeader getCanalEntryHeader(CanalEntry.Header header) {
        CanalEntryHeader canalEntryHeader = new CanalEntryHeader();
        if(Objects.isNull(header)){
            return canalEntryHeader;
        }
        canalEntryHeader.setServerId(header.getServerId());
        canalEntryHeader.setSchemaName(header.getSchemaName());
        canalEntryHeader.setTableName(header.getTableName());
        canalEntryHeader.setHandlerType(HandlerUtils.transform(header.getEventType()));
        canalEntryHeader.setExecuteTime(header.getExecuteTime());
        return canalEntryHeader;
    }

    /**
     * 获取CanalEntryColumn
     *
     * @param columnList
     * @return
     */
    private List<CanalEntryColumn> getCanalEntryColumnList(List<CanalEntry.Column> columnList) {
        List<CanalEntryColumn> canalEntryColumnList  = Lists.newArrayList();
        if(CollectionUtils.isEmpty(columnList)){
            return canalEntryColumnList;
        }
        CanalEntryColumn canalEntryColumn;
        for (CanalEntry.Column column : columnList) {
            canalEntryColumn = new CanalEntryColumn()
                    .setName(column.getName())
                    .setValue(column.getValue())
                    .setUpdated(column.getUpdated())
                    .setIsKey(column.getIsKey())
                    .setIsNull(column.getIsNull())
                    .setMysqlType(column.getMysqlType());
            canalEntryColumnList.add(canalEntryColumn);
        }
        return canalEntryColumnList;
    }

    /**
     * 容器销毁 释放资源
     *
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        log.info("Canal Client RocketMQ Runner 准备销毁");
        if (running) {
            running = false;
            if (Objects.nonNull(connector)) {
                connector.unsubscribe();
                connector.disconnect();
            }
        }
        log.info("Canal Client RocketMQ Runner 完成销毁");
    }

    /**
     * canal entry column
     */
    @Data
    @Accessors(chain = true)
    private static class CanalEntryColumn{
        private String name;
        private String value;
        private Boolean updated;
        private Boolean isKey;
        private Boolean isNull;
        private String mysqlType;
    }

    /**
     * canal entry header
     */
    @Data
    @Accessors(chain = true)
    private static class CanalEntryHeader{
        private Long serverId;
        private Long executeTime;
        private Long timestamp;
        private String schemaName;
        private String tableName;
        private HandlerTypeEnum handlerType;
    }
}
