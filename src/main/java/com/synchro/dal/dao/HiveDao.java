package com.synchro.dal.dao;

import com.synchro.dal.libs.JdbcTemplateFactory;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.TableMetaData;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Repository
public class HiveDao {

	private final static Logger logger = LoggerFactory.getLogger(HiveDao.class);

	/**
	 * 创建数据表
	 * 
	 * @param tableMetaData
	 * @return
	 */
	public boolean createTable(JdbcTemplate hiveJdbcTemplate, TableMetaData tableMetaData) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE IF NOT EXISTS " + tableMetaData.getSchema() + "." + tableMetaData.getTableName() + "(");
		for (ColumnMetaData columnMetaData : tableMetaData.getColumnMetaDatas()) {
			if (columnMetaData.getTypeName().equals("int")) {
				sql.append(columnMetaData.getName() + " int,");
			} else if (columnMetaData.getTypeName().equals("bigint")) {
				sql.append(columnMetaData.getName() + " bigint,");
			} else {
				sql.append(columnMetaData.getName() + " string,");
			}
		}
		if (sql.charAt(sql.length() - 1) == ',') {
			sql.deleteCharAt(sql.length() - 1);
		}
		sql.append(") ");
		// 分区字段
		if (tableMetaData.getPartitionColumn() != null && !tableMetaData.getPartitionColumn().equals("")) {
			sql.append("PARTITIONED BY (" + tableMetaData.getPartitionColumn() + " string)");
		}
		sql.append("LOCATION 'hdfs://*****cluster/user/ticketdev/hive/warehouse/" + tableMetaData.getSchema() + ".db/" + tableMetaData.getTableName() + "'");
		logger.info(sql.toString());
		hiveJdbcTemplate.execute(sql.toString());
		return true;
	}

	/**
	 * truncat数据表
	 * 
	 * @param tableMetaData
	 * @return
	 */
	public synchronized boolean truncatTable(JdbcTemplate hiveJdbcTemplate, TableMetaData tableMetaData) {
		String sql = "TRUNCATE TABLE " + tableMetaData.getTableName();
		logger.info(sql);
		hiveJdbcTemplate.execute("USE " + tableMetaData.getSchema());
		hiveJdbcTemplate.execute(sql);
		return true;
	}

	public synchronized boolean dropTablePartition(JdbcTemplate hiveJdbcTemplate, TableMetaData tableMetaData, String partionValue) {
		String sql = "ALTER TABLE " + tableMetaData.getTableName() + " DROP IF EXISTS  PARTITION (rpt_date='" + partionValue + "')";
		logger.info(sql);
		hiveJdbcTemplate.execute("USE " + tableMetaData.getSchema());
		hiveJdbcTemplate.execute(sql);
		return true;
	}

	/**
	 * 删除数据表
	 * 
	 * @param tableMetaData
	 * @return
	 */
	public boolean dropTable(JdbcTemplate hiveJdbcTemplate, TableMetaData tableMetaData) {
		String sql = "DROP TABLE IF EXISTS " + tableMetaData.getSchema() + "." + tableMetaData.getTableName();
		hiveJdbcTemplate.execute(sql);
		return true;
	}

	/**
	 * 查看数据表是否存在
	 * 
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public boolean checkTableExit(JdbcTemplate hiveJdbcTemplate, String databaseName, String tableName) {
		logger.info("SHOW TABLES LIKE '" + databaseName +"." + tableName + "'");
		hiveJdbcTemplate.execute("use " + databaseName);
		List<Map<String, Object>> t = hiveJdbcTemplate.queryForList("SHOW TABLES LIKE '" + tableName + "'");
		return t.size() > 0;
	}

	/**
	 * 查看数据表结构
	 * 
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	public List<Map<String, Object>> getCreateTable(JdbcTemplate hiveJdbcTemplate, String databaseName, String tableName) {
		hiveJdbcTemplate.execute("use " + databaseName);
		List<Map<String, Object>> t = hiveJdbcTemplate.queryForList("desc " + tableName);
		return t;
	}

	/**
	 * 获取表结构
	 * 
	 * @return
	 */
	public List<ColumnMetaData> getColumnMetaData(JdbcTemplate hiveJdbcTemplate, String databaseName, String tableName) {
		if (!this.checkTableExit(hiveJdbcTemplate,  databaseName,  tableName)){
			// 如果数据表不存在
			return null;
		}
		List<Map<String, Object>> t = hiveJdbcTemplate.queryForList("desc " + databaseName + "." + tableName);
		List<ColumnMetaData> columneMetaList = new LinkedList<ColumnMetaData>();
		for (Map<String, Object> tmp : t) {
			if (tmp.get("col_name") == null || tmp.get("data_type") == null || tmp.get("data_type").toString().trim().equals("data_type"))
				break;
			columneMetaList.add(new ColumnMetaData(tmp.get("col_name").toString().trim(), 0, tmp.get("data_type").toString().trim()));
		}
		Collections.sort(columneMetaList);
		return columneMetaList;
	}

	/**
	 * 导入文件到hive中
	 * 
	 * @param filePath
	 * @param databaseName
	 * @param tableName
	 */
	public void loadFileToHive(String dataSourceName, String filePath, String databaseName, String tableName, String partitionColumnValue) {
		long t0 = System.currentTimeMillis();
		String insterSQL = "LOAD DATA  INPATH '" + filePath + "'  INTO TABLE " + databaseName + "." + tableName;
		if (StringUtils.isNotEmpty(partitionColumnValue)) {
			insterSQL = insterSQL + " partition (rpt_date = '" + partitionColumnValue + "')";
		}
		logger.info(insterSQL);
		JdbcTemplate jdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplateFromDataSourceName(dataSourceName);
		jdbcTemplate.execute(insterSQL);
		long t1 = System.currentTimeMillis();
		logger.info("loadFileToHive insterSQL[{}] {}秒", insterSQL, (t1 - t0) / 1000);
	}

}
