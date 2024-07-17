package com.dongwo.data.tunnel.alarm;

import com.alibaba.fastjson.JSON;
import com.dongwo.data.tunnel.canal.config.CanalTunnelConfig;
import com.dongwo.data.tunnel.mapper.CanalTunnelFailureRecordMapper;
import com.dongwo.data.tunnel.mapper.CanalTunnelFailureRecordQuery;
import com.dongwo.data.tunnel.po.CanalTunnelFailureRecordPO;
import com.easyhome.common.timer.JavaGrayProcessor;
import com.easyhome.common.timer.JobContext;
import com.easyhome.common.timer.ProcessResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 通知告警 同步Es失败的记录
 *
 * @author : Leon on XXM Mac
 * @since : create in 2024/4/17 2:55 PM
 */
@Slf4j
@Component
public abstract class AbstractFailureAlarmProcessorBasic extends JavaGrayProcessor {

    /**
     * 告警配置
     */
    @Resource
    private CanalTunnelConfig canalTunnelConfig;

    /**
     * restTemplate
     */
    @Resource
    private RestTemplate restTemplate;

    /**
     * 告警记录
     */
    @Resource
    private CanalTunnelFailureRecordMapper canalTunnelFailureRecordMapper;

    /**
     * 任务
     *
     * @param jobContext
     * @return
     * @throws Exception
     */
    @Override
    public ProcessResult process(JobContext jobContext) throws Exception {
        String processorName = processorName();
        log.info("======== {} : FailureAlarmJob process entry ========", processorName);
        this.sendRobot();
        log.info("======== {} : FailureAlarmJob process finish ========", processorName);
        return new ProcessResult(true, "通知告警:同步Es失败的记录");
    }

    /**
     * 处理器名称
     *
     * @return
     */
    public abstract String processorName();


    /**
     * 发送钉钉机器人
     */
    private void sendRobot() {
        String url = canalTunnelConfig.getFallback().getAlarm().getWebhook();
        if(StringUtils.isBlank(url)){
            log.info("======== FailureAlarmJob no webhook send ========");
            return;
        }
        String msg = this.getMsg();
        if(StringUtils.isBlank(msg)){
            log.info("======== FailureAlarmJob no msg send ========");
            return;
        }
        LinkedMultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
        headers.put("Content-Type", Collections.singletonList("application/json"));
        Map<String, String> body = new HashMap<>(4);
        body.put("msgtype", "text");
        body.put("text", JSON.toJSONString(Collections.singletonMap("content",  msg)));
        HttpEntity httpEntity = new HttpEntity(body, headers);

        restTemplate.exchange(url, HttpMethod.POST, httpEntity, String.class);
    }


    /**
     * 告警的snTn消息
     *
     * @return
     */
    private String getMsg() {
        Long descRecordId = Long.MAX_VALUE, limit = 100L;
        List<CanalTunnelFailureRecordPO> canalTunnelFailureRecordPOList;
        Set<String> alarmSnTnAll = new HashSet<>();
        List<CanalTunnelFailureRecordQuery.SnTn> snTnList = this.getSnTnList();
        CanalTunnelFailureRecordQuery alarmQuery = new CanalTunnelFailureRecordQuery().setDescRecordId(descRecordId).setLimit(limit).setSnTnList(snTnList);
        for (;CollectionUtils.isNotEmpty(canalTunnelFailureRecordPOList = canalTunnelFailureRecordMapper.listAlarm(alarmQuery));){
            Set<String> alarmSnTn = canalTunnelFailureRecordPOList
                    .stream()
                    .map(ctfrp -> ctfrp.getSchemaName().concat(".").concat(ctfrp.getTableName()))
                    .collect(Collectors.toSet());
            alarmSnTnAll.addAll(alarmSnTn);
            if(!Objects.equals(canalTunnelFailureRecordPOList.size(), limit.intValue())){
                // 不足说明后边没有了
                break;
            }
            descRecordId = canalTunnelFailureRecordPOList.stream().map(CanalTunnelFailureRecordPO::getRecordId).min(Comparator.naturalOrder()).orElse(Long.MIN_VALUE);
            alarmQuery.setDescRecordId(descRecordId);
        }
        if(CollectionUtils.isEmpty(alarmSnTnAll)){
            return null;
        }
        String msg = String.format("数据同步告警:Tunnel同步ES存在失败情况，需要人工介入。alarmSnTn : %s", JSON.toJSONString(alarmSnTnAll));
        return msg;
    }

    /**
     * 告警的snTn
     *
     * @return
     */
    private List<CanalTunnelFailureRecordQuery.SnTn> getSnTnList() {
        List<CanalTunnelFailureRecordQuery.SnTn> snTnList = Lists.newArrayList();
        for (Map.Entry<String, List<String>> entry : canalTunnelConfig.getFallback().getAlarm().getProcessor().entrySet()) {
            for (String tns : entry.getValue()) {
                CanalTunnelFailureRecordQuery.SnTn snTn = new CanalTunnelFailureRecordQuery.SnTn().setSchemaName(entry.getKey()).setTableName(tns);
                snTnList.add(snTn);
            }
        }
        return snTnList;
    }
}
