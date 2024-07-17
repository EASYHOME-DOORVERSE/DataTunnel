package com.dongwo.data.tunnel.mapper;

import com.dongwo.data.tunnel.po.CanalTunnelFailureRecordPO;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/29 3:23 PM
 */
@Mapper
public interface CanalTunnelFailureRecordMapper {

    /**
     * insert
     *
     * @param canalTunnelFailureRecordPO
     * @return
     */
    @Insert("insert into canal_tunnel_failure_record " +
            "(record_id, schema_name, table_name, handler_type, handler_clazz, entry, before_columns_list, after_columns_list) " +
            "values " +
            "(#{po.recordId}, #{po.schemaName}, #{po.tableName}, #{po.handlerType}, #{po.handlerClazz}, #{po.entry}, #{po.beforeColumnsList}, #{po.afterColumnsList})")
    int insert(@Param("po") CanalTunnelFailureRecordPO canalTunnelFailureRecordPO);


    /**
     * listAlarm
     *
     * @param alarmQuery
     * @return
     */
    @SelectProvider(type = CanalTunnelFailureRecordProvider.class, method = "listAlarm")
    List<CanalTunnelFailureRecordPO> listAlarm(@Param("alarmQuery") CanalTunnelFailureRecordQuery alarmQuery);
}
