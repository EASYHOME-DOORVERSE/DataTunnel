package com.dongwo.data.tunnel.starter.selector;

import com.dongwo.data.tunnel.canal.config.CanalTunnelConfig;
import com.dongwo.data.tunnel.canal.factory.HandlerFactory;
import com.dongwo.data.tunnel.canal.runner.CanalClientRocketMQFlatMessageRunner;
import com.dongwo.data.tunnel.canal.runner.CanalClientRocketMQRunner;
import com.dongwo.data.tunnel.config.CanalTunnelFailureRecordMapperOpen;
import com.dongwo.data.tunnel.config.ElasticSearchConfig;
import com.dongwo.data.tunnel.manager.CanalTunnelFailureRecordManager;
import com.dongwo.data.tunnel.mapper.CanalTunnelFailureRecordProvider;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/3/4 4:06 PM
 */
public class CanalTunnelImportSelector implements ImportSelector {

    /**
     * 注入组件
     * @param importingClassMetadata
     * @return
     */
    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        return new String[]{
                CanalTunnelConfig.class.getName(),
                ElasticSearchConfig.class.getName(),
                HandlerFactory.class.getName(),
                CanalTunnelFailureRecordManager.class.getName(),
                CanalTunnelFailureRecordMapperOpen.class.getName(),
                CanalClientRocketMQRunner.class.getName(),
                CanalClientRocketMQFlatMessageRunner.class.getName()
        };
    }
}
