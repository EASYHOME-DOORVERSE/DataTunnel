package com.dongwo.data.tunnel.canal.runner;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.dongwo.data.tunnel.canal.config.CanalTunnelConfig;
import com.dongwo.data.tunnel.canal.factory.HandlerFactory;
import com.dongwo.data.tunnel.canal.handler.flatmessage.FlatMessageEntryHandler;
import com.dongwo.data.tunnel.canal.utils.CanalNamedThreadFactory;
import com.dongwo.data.tunnel.canal.utils.HandlerUtils;
import com.dongwo.data.tunnel.manager.CanalTunnelFailureRecordManager;
import com.dongwo.data.tunnel.po.CanalTunnelFailureRecordPO;
import com.easyhome.common.utils.SnowflakeIdWorker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
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
@ConditionalOnProperty(value = "tunnel.rocketMq.flatMessage", havingValue = "true")
public class CanalClientRocketMQFlatMessageRunner implements ApplicationRunner, DisposableBean {

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
     * canal 错误记录管理器
     */
    @Resource
    private CanalTunnelFailureRecordManager canalTunnelFailureRecordManager;

    /**
     * 启动成功
     */
    private volatile boolean running = false;

    /**
     *
     */
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, new CanalNamedThreadFactory("CanalConnector-withoutAck-flatMessage"));


    private RocketMQCanalConnector connector;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Canal Client FlatMessage RocketMQ Runner 开始启动");
        CanalTunnelConfig.RocketMq rocketMQConfig = canalTunnelConfig.getRocketMq();
        log.info("Canal Client FlatMessage RocketMQ Runner 配置信息 : {}", JSON.toJSONString(rocketMQConfig));
        // 创建链接
        connector = new RocketMQCanalConnector(
                rocketMQConfig.getNameServers(), rocketMQConfig.getTopic(), rocketMQConfig.getConsumer().getGroupId(),
                null, null,
                -1, true, false,
                null, "local", "");
        connector.connect();
        connector.subscribe();
        connector.rollback();
        running = true;
        log.info("Canal Client FlatMessage RocketMQ Runner 开始调度");
        executor.scheduleAtFixedRate(() -> {
            try {
                if (!running) {
                    log.info("Canal Client FlatMessage RocketMQ Runner 停止调度");
                    return;
                }
                List<FlatMessage> messages = connector.getFlatList(100L, TimeUnit.MILLISECONDS); // 获取message
                for (FlatMessage message : messages) {
                    long batchId = message.getId();
                    List<Map<String, String>> data = message.getData();
                    if (batchId != -1 && CollectionUtils.isNotEmpty(data)) {
                        this.printEntryFlatMessage(message);
                    }
                }
                connector.ack();
            }catch (Throwable e){
                log.info("Canal Client FlatMessage RocketMQ Runner 防止异常未捕获导致任务停止. Error : {}", e.getMessage(), e);
            }
        }, 0, 1, TimeUnit.SECONDS);
        log.info("Canal Client FlatMessage RocketMQ Runner 完成调度");
    }


    /**
     * 打印条目
     *
     * @param message
     */
    private void printEntryFlatMessage(FlatMessage message) {
        try {
                List<FlatMessageEntryHandler> entryHandlers = handlerFactory.getFlatMessageHandlers(message.getDatabase(), message.getTable());
                List<Map<String, String>> before = null, after = null;
                for (FlatMessageEntryHandler entryHandler : entryHandlers) {
                    try {
                        switch (message.getType()){
                            case "INSERT":
                                entryHandler.handleInsert(message.getData());
                                after = message.getData();
                                break;
                            case "UPDATE":
                                entryHandler.handleUpdate(message.getOld(), message.getData());
                                before = message.getOld();
                                after = message.getData();
                                break;
                            case "DELETE":
                                entryHandler.handleDelete(message.getData());
                                before = message.getData();
                                break;
                            default:
                                break;
                        }
                    } catch (Throwable e) {
                        log.info("Canal Client FlatMessage RocketMQ Runner CanalEntry 处理异常 message : {}, EntryHandler : {}", JSON.toJSONString(message), entryHandler.getClass().getName(), e);
                        CanalTunnelFailureRecordPO recordPO = new CanalTunnelFailureRecordPO()
                                .setRecordId(SnowflakeIdWorker.generateId())
                                .setSchemaName(message.getDatabase())
                                .setTableName(message.getTable())
                                .setHandlerType(HandlerUtils.transform(message.getType()))
                                .setHandlerClazz(entryHandler.getClass().getName())
                                .setEntry(JSON.toJSONString(message))
                                .setBeforeColumnsList(JSON.toJSONString(before)).setAfterColumnsList(JSON.toJSONString(after));

                        canalTunnelFailureRecordManager.record(recordPO);
                    }
                }
        }catch (Exception e){
            log.info("Canal Client RocketMQ Runner CanalEntry 打印异常 Error : {}", e.getMessage(), e);
        }
    }

    @Override
    public void destroy() throws Exception {
        log.info("Canal Client FlatMessage RocketMQ Runner 准备销毁");
        if (running) {
            running = false;
            if (Objects.nonNull(connector)) {
                connector.unsubscribe();
                connector.disconnect();
            }
        }
        log.info("Canal Client FlatMessage RocketMQ Runner 完成销毁");
    }

}
