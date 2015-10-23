package synchro;

import com.synchro.common.constant.SyncConstant;
import com.synchro.dal.metadata.DataBaseTypeMetaData;
import com.synchro.dal.metadata.DataSourceMetaData;
import com.synchro.service.DataSourceService;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

/**
 * Created by qiu.li on 2015/10/22.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring.xml")
public class FetchSize {

    @Resource
    private DataSourceService dataSourceService;

    @Test
    public void getData() {
        DataSourceMetaData dataSource = dataSourceService.getDataSource("log_analysis");
        DataBaseTypeMetaData dbType = dataSource.getDataBaseType();
        BasicDataSource bds = new BasicDataSource();
        bds.setDriverClassName(dbType.getDriverClass());
        bds.setUrl(dataSource.getConnectionUrl());
        bds.setUsername(dataSource.getUserName());
        bds.setPassword(dataSource.getPassword());
        bds.setInitialSize(1); // 初始化连接数量
        bds.setMaxActive(1); // 最大链接数量
        bds.setMinIdle(1); // 最小空闲链接
        bds.setMaxIdle(0); // 最大空闲链接
        bds.setDefaultAutoCommit(false);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(bds);
        jdbcTemplate.setFetchSize(100);
        String sql = "select * from mirror.b2c_product_ticket_date";
        SqlRowSet resultSet = jdbcTemplate.queryForRowSet(sql);
        while (resultSet.next()) {
            resultSet.getString(0);
            return ;
        }
    }
}
