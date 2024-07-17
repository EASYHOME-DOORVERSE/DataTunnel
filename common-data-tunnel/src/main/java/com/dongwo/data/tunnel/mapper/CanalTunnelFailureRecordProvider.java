package com.dongwo.data.tunnel.mapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.jdbc.SQL;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/4/18 1:43 PM
 */
public class CanalTunnelFailureRecordProvider {

    /**
     * 获取报警
     *
     * @param alarmQuery
     * @return
     */
    public String listAlarm(@Param("alarmQuery") CanalTunnelFailureRecordQuery alarmQuery){
        return new SQL(){
            {
                SELECT("schema_name AS schemaName", "table_name as tableName", "record_id as recordId");
                FROM("canal_tunnel_failure_record");
                WHERE("done = false");
                WHERE(String.format("record_id < %s", alarmQuery.getDescRecordId().toString()));
                if(CollectionUtils.isNotEmpty(alarmQuery.getSnTnList())){
                    String snTnCondition =
                            alarmQuery.getSnTnList()
                                    .stream()
                                    .map(snTn -> String.format("('%s', '%s')", snTn.getSchemaName(),  snTn.getTableName()))
                                    .reduce((acc, add) -> String.format("%s, %s", acc, add))
                                    .get();
                    WHERE(String.format("(schema_name, table_name) in (%s)", snTnCondition));
                }
                ORDER_BY("record_id desc");
                LIMIT(alarmQuery.getLimit().toString());
            }
        }.toString();
    }
}
