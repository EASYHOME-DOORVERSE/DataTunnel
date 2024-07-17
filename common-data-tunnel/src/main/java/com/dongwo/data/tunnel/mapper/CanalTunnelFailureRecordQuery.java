package com.dongwo.data.tunnel.mapper;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/4/18 1:55 PM
 */
@Data
@Accessors(chain = true)
public class CanalTunnelFailureRecordQuery {
    private Long descRecordId;
    private Long limit;
    private List<SnTn> snTnList;

    @Data
    @Accessors(chain = true)
    public static class SnTn{
        private String schemaName;
        private String tableName;
    }
}
