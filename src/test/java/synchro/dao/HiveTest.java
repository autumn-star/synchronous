package synchro.dao;

import com.synchro.dal.dao.HiveDao;
import com.synchro.service.DataSourceService;
import com.synchro.dal.libs.JdbcTemplateFactory;
import com.synchro.dal.metadata.DataSourceMetaData;
import com.synchro.dal.metadata.TableMetaData;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.PostgresToHiveWorker;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

//import org.apache.hadoop.hive.jdbc.HiveResultSetMetaData;

//import org.apache.hadoop.hive.jdbc.HiveResultSetMetaData;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring.xml")
public class HiveTest {
	
	@Resource
	HiveDao hiveDao;

	@Autowired
	PostgresToHiveWorker fromPgToHiveService;

	@Autowired
	private DataSourceService dataSourceService;
	
	JdbcTemplate jdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplate(dataSourceService.getDataSource("hive"));

	@Test
	@Ignore
	public void testMap() {
		String sql = "select * from tmp.test_hstore_info limit 2";
		List<Map<String, Object>> m = jdbcTemplate.queryForList(sql);
		System.out.println(m);
	}

	@Test
	@Ignore
	public void testSet() {
		DataSourceMetaData dataSource = dataSourceService.getDataSource("hive");
		JdbcTemplate jdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplate(dataSource);
		String sql = "select * from tmp.column_row limit 2";
		jdbcTemplate.execute("set hive.cli.print.row.to.vertical=true;");
		jdbcTemplate.queryForObject(sql, new RowMapper<ResultSet>() {

			@Override
			public ResultSet mapRow(ResultSet rs, int rowNum) throws SQLException {
				while (rs.next()) {
					System.out.println("--------" + rs.getString("col3"));
				}
				return rs;
			}

		});
	}

	@Test
	@Ignore
	public void testColumnMeta() {
		DataSourceMetaData dataSource = dataSourceService.getDataSource("hive");
		JdbcTemplate jdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplate(dataSource);
		String sql = "select * from tmp.test_hstore_info";

		jdbcTemplate.queryForObject(sql, new RowMapper<ResultSet>() {
			@Override
			public ResultSet mapRow(ResultSet rs, int rowNum) throws SQLException {
				while (rs.next()) {
					System.out.println(rs.getObject("trace_url_info"));
					ResultSetMetaData sqlRowSetMetaData = rs.getMetaData();
					int num = sqlRowSetMetaData.getColumnCount();
					for (int i = 1; i <= num; i++) {
						System.out.println(i + "column:" + sqlRowSetMetaData.getColumnLabel(i));
						System.out.println(i + "columnType:" + sqlRowSetMetaData.getColumnTypeName(i));
					}
				}
				return rs;
			}

		});
	}
	
	@Test
	@Ignore
	public void testCreateTable(){
		TableMetaData tableMetaData = new TableMetaData();
		tableMetaData.setTableName("test");
		tableMetaData.setSchema("mirror");
		/*ColumnMetaData columnMetaData0 = new ColumnMetaData();
		columnMetaData0.setName("test");
		columnMetaData0.setTypeName("int");
		tableMetaData.addColumnMetaData(columnMetaData0);
		
		ColumnMetaData columnMetaData1 = new ColumnMetaData();
		columnMetaData1.setName("test2");
		columnMetaData1.setTypeName("bigint");
		tableMetaData.addColumnMetaData(columnMetaData1);
		
		ColumnMetaData columnMetaData2 = new ColumnMetaData();
		columnMetaData2.setName("test3");
		columnMetaData2.setTypeName("varchar");
		tableMetaData.addColumnMetaData(columnMetaData2);*/
		DataSourceMetaData dataSource = dataSourceService.getDataSource("hive");
		JdbcTemplate jdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplate(dataSource);

		hiveDao.createTable(jdbcTemplate,tableMetaData);
	}

	@Test
	@Ignore
	public void testCreateFile() throws Exception {

		SyncOptionsDto fromPgToHiveDto = new SyncOptionsDto();
		fromPgToHiveDto.setSrcSchemaName("public");
		fromPgToHiveDto.setSrcDataSourceName("log_analysis");
		fromPgToHiveDto.setSrcTableName("sight");
		fromPgToHiveDto.setTgtDataSourceName("test");
		fromPgToHiveDto.setTgtDataSourceName("hive");
		fromPgToHiveDto.setTgtTableName("sight");
	}

	@Ignore
	public void testDropTable(){
		TableMetaData tableMetaData = new TableMetaData();
		tableMetaData.setTableName("test");
		tableMetaData.setSchema("mirror");
		hiveDao.dropTable(jdbcTemplate, tableMetaData);
	}

	@Test
	@Ignore
	public void testCheckTableExit(){
		boolean check = hiveDao.checkTableExit(jdbcTemplate, "mirror","sight");
		System.out.println(check);
	}

	@Test
	@Ignore
	public void testGetCreateTable(){
		List<Map<String, Object>> t = hiveDao.getCreateTable(jdbcTemplate, "mirror", "test2");
		System.out.println(t);
	}
}