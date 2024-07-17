package com.dongwo.data.tunnel.manager;

import com.alibaba.fastjson2.JSON;
import com.dongwo.data.tunnel.canal.config.CanalTunnelConfig;
import com.dongwo.data.tunnel.mapper.CanalTunnelFailureRecordMapper;
import com.dongwo.data.tunnel.po.CanalTunnelFailureRecordPO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/29 3:13 PM
 */
@Slf4j
@Component
public class CanalTunnelFailureRecordManager {

    /**
     * 失败回调Mapper
     */
    @Autowired
    private CanalTunnelFailureRecordMapper canalTunnelFailureRecordMapper;

    /**
     * canalTunnel配置
     */
    @Resource
    private CanalTunnelConfig canalTunnelConfig;

    /**
     * 记录失败
     *
     * @param canalTunnelFailureRecordPO
     * @return 影响行数
     */
    public Integer record(CanalTunnelFailureRecordPO canalTunnelFailureRecordPO) {
        try {
            int rows = 0;
            if(canalTunnelConfig.getFallback().getActivate()) {
                rows = canalTunnelFailureRecordMapper.insert(canalTunnelFailureRecordPO);
            }else{
                log.warn("记录失败-canalTunnelFailureRecordPO : {}", JSON.toJSONString(canalTunnelFailureRecordPO));
            }
            return rows;
        }catch (Throwable e){
            log.error("记录失败-canalTunnelFailureRecordPO : {}", JSON.toJSONString(canalTunnelFailureRecordPO), e);
        }
        return 0;
    }
}
