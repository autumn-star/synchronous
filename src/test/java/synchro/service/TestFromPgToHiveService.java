package synchro.service;

import com.synchro.dal.libs.JdbcTemplateFactory;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.TableMetaData;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.PostgresToHiveWorker;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;






//import java.sql.Types;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring.xml")
public class TestFromPgToHiveService {

	private final static Logger LOGGER = LoggerFactory.getLogger(TestFromPgToHiveService.class);

	@Resource
	PostgresToHiveWorker fromPgToHiveService;

	@Test
	public void getFormPgToHive() {
		SyncOptionsDto fromPgToHiveDto = new SyncOptionsDto();
		fromPgToHiveDto.setSrcDataSourceName("src_tts_online");
		fromPgToHiveDto.setSrcSchemaName("public");
		fromPgToHiveDto.setSrcTableName("b2c_product_ticket_price");
		fromPgToHiveDto.setPartitionColumnName("operate_time");
		fromPgToHiveDto.setTgtDataSourceName("hive");
		fromPgToHiveDto.setTgtSchemaName("business_mirror");
		fromPgToHiveDto.setTgtDataSourceName("business_mirror");
		fromPgToHiveDto.setTgtTableName("b2c_product_ticket_price");
		try {
			fromPgToHiveService.setOptions(fromPgToHiveDto);
			fromPgToHiveService.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 测试postgresql里面的特殊字段问题
	 */
	@Test
	@Ignore
	public void testSpecialColumn() {
		// 设置基本信息
		SyncOptionsDto fromPgToHiveDto = new SyncOptionsDto();
		fromPgToHiveDto.setSrcDataSourceName("src_tts_online");
		fromPgToHiveDto.setSrcSchemaName("public");
		fromPgToHiveDto.setSrcTableName("b2c_product_summary");
		JdbcTemplate srcJdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplateFromDataSourceName(fromPgToHiveDto.getSrcDataSourceName());
		// 获取数据表内容
		TableMetaData srcTableMetaData = new TableMetaData();
		String sql = "select " + srcTableMetaData.getColumns() + " from b2c_product_summary limit 40";
		SqlRowSet set = srcJdbcTemplate.queryForRowSet(sql);
		try {
			List<ColumnMetaData> columns = srcTableMetaData.getColumnMetaDatas();
			while (set.next()) {
				for (ColumnMetaData column : columns) {
					Object value = set.getObject(column.getName());
					System.out.println(column.getName() + "-" + value);
				}
			}
		} catch (Exception ex) {
			LOGGER.error("文件创建失败！", ex);
		}
	}

	@Test
	@Ignore
	public void testLimit() {
		JdbcTemplate srcJdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplateFromDataSourceName("log_analysis");
		long t0 = System.currentTimeMillis();
		srcJdbcTemplate.queryForRowSet("SELECT * FROM dw.sp_sight_view_history where stat_date='2015-05-01'");
		long t1 = System.currentTimeMillis();
		LOGGER.info("查询耗时 {} 秒", (t1 - t0) / 1000);
	}

}
