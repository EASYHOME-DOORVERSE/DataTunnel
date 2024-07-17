package com.dongwo.data.tunnel.mapper;

import com.dongwo.data.tunnel.canal.enums.HandlerTypeEnum;
import com.dongwo.data.tunnel.canal.handler.AbstractGeneralEntryHandler;
import com.dongwo.data.tunnel.po.CanalTunnelFailureRecordPO;
import com.easyhome.common.utils.SnowflakeIdWorker;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/4/18 2:07 PM
 */
@Slf4j
public class CanalTunnelFailureRecordMapperTest {

    /**
     * mapper
     */
    private static CanalTunnelFailureRecordMapper mapper;

    /**
     * 测试初始化
     */
    @BeforeClass
    public static void setUpMybatisDatabase() {
        SqlSessionFactory builder = new SqlSessionFactoryBuilder().build(CanalTunnelFailureRecordMapperTest.class.getClassLoader().getResourceAsStream("mybatisTestConfiguration/CanalTunnelFailureRecordMapperTestConfiguration.xml"));
        //you can use builder.openSession(false) to not commit to database
        mapper = builder.getConfiguration().getMapper(CanalTunnelFailureRecordMapper.class, builder.openSession(true));

    }

    /**
     * 测试插入
     */
    @Test
    public void testInsert() {
        {
            CanalTunnelFailureRecordPO ctfrp = new CanalTunnelFailureRecordPO()
                    .setRecordId(SnowflakeIdWorker.generateId())
                    .setSchemaName("zoo")
                    .setTableName("monkey")
                    .setHandlerType(HandlerTypeEnum.INSERT)
                    .setHandlerClazz(AbstractGeneralEntryHandler.class.getName())
                    .setEntry("{\"schema\": \"zoo\"}")
                    .setBeforeColumnsList("[{\"name\": \"id\"}]")
                    .setAfterColumnsList("[{\"name\": \"id\"}]")
                    .setMgtCreated(new Date());
            int rows = mapper.insert(ctfrp);
            Assert.assertEquals(1, rows);
        }
        {
            CanalTunnelFailureRecordPO ctfrp = new CanalTunnelFailureRecordPO()
                    .setRecordId(SnowflakeIdWorker.generateId())
                    .setSchemaName("zoo")
                    .setTableName("donkey")
                    .setHandlerType(HandlerTypeEnum.INSERT)
                    .setHandlerClazz(AbstractGeneralEntryHandler.class.getName())
                    .setEntry("{\"schema\": \"zoo\"}")
                    .setBeforeColumnsList("[{\"name\": \"id\"}]")
                    .setAfterColumnsList("[{\"name\": \"id\"}]")
                    .setMgtCreated(new Date());
            int rows = mapper.insert(ctfrp);
            Assert.assertEquals(1, rows);
        }
    }


    /**
     * 测试查询
     */
    @Test
    public void testListAlarm() {
        CanalTunnelFailureRecordQuery ctfrq =
                new CanalTunnelFailureRecordQuery()
                        .setDescRecordId(Long.MAX_VALUE)
                        .setLimit(100L)
                        .setSnTnList(
                                Arrays.asList(
                                        new CanalTunnelFailureRecordQuery
                                                .SnTn()
                                                .setSchemaName("zoo")
                                                .setTableName("monkey"),
                                        new CanalTunnelFailureRecordQuery
                                                .SnTn()
                                                .setSchemaName("zoo")
                                                .setTableName("donkey")
                                )
                        );
        List<CanalTunnelFailureRecordPO> result = mapper.listAlarm(ctfrq);
        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.size());
        for (CanalTunnelFailureRecordPO canalTunnelFailureRecordPO : result) {
            Assert.assertNotNull(canalTunnelFailureRecordPO);
            Assert.assertNotNull(canalTunnelFailureRecordPO.getRecordId());
            Assert.assertEquals(canalTunnelFailureRecordPO.getSchemaName(), "zoo");
            Assert.assertThat(canalTunnelFailureRecordPO.getTableName(), Matchers.anyOf(Matchers.is("monkey"), Matchers.is("donkey")));
        }

    }

}
