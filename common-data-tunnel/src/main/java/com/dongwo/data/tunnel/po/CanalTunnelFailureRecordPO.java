package com.dongwo.data.tunnel.po;

import com.dongwo.data.tunnel.canal.enums.HandlerTypeEnum;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Date;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/29 3:16 PM
 */
@Data
@Accessors(chain = true)
public class CanalTunnelFailureRecordPO {
    /**
     * 主键
     */
    private Long id;

    /**
     * 业务主键
     */
    private Long recordId;

    /**
     * schema name
     */
    private String schemaName;

    /**
     * table name
     */
    private String tableName;

    /**
     * event type
     */
    private HandlerTypeEnum handlerType;

    /**
     * binlog 记录的业务id
     */
    private String handlerClazz;

    /**
     * binlog entry
     */
    private String entry;

    /**
     * 处理前的列
     */
    private String beforeColumnsList;

    /**
     * 处理后的列
     */
    private String afterColumnsList;

    /**
     * 处理完成
     */
    private Boolean done;

    /**
     * 创建时间
     */
    private Date mgtCreated;
}
