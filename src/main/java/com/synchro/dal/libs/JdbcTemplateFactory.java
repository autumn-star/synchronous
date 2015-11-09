package com.synchro.dal.libs;

import com.synchro.dal.metadata.DataBaseTypeMetaData;
import com.synchro.dal.metadata.DataSourceMetaData;
import com.synchro.common.constant.SyncConstant;

import com.synchro.service.DataSourceService;
import com.synchro.util.SpringContextUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 工厂模式，产出BasicDataSource
 * 
 * @author qiu.li
 * 
 */
public class JdbcTemplateFactory {

	private static JdbcTemplateFactory INSTANCE; // 单例

	private final Map<String, JdbcTemplate> jdbcTemplateMap = Collections.synchronizedMap(new HashMap<String, JdbcTemplate>()); // 防止高并发、多线程产生错误

	private DataSourceService dataSourceService;

	// 防止被new
	private JdbcTemplateFactory() {

	}

	public void setDataSourceService(DataSourceService dataSourceService) {
		this.dataSourceService = dataSourceService;
	}

	/**
	 * 单例模式懒汉模式
	 */
	public synchronized static JdbcTemplateFactory getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new JdbcTemplateFactory();
			INSTANCE.setDataSourceService(SpringContextUtils.getBean(DataSourceService.class));
		}
		return INSTANCE;
	}

	// 单线程，防止并发出现多次创建的问题
	public synchronized JdbcTemplate getJdbcTemplate(DataSourceMetaData dataSource) {
		JdbcTemplate jdbcTemplate;
		if (jdbcTemplateMap.containsKey(dataSource.getDataSourceName())) {
			jdbcTemplate = jdbcTemplateMap.get(dataSource.getDataSourceName());
		} else {
			jdbcTemplate = buildJdbcTemplate(dataSource);
			jdbcTemplateMap.put(dataSource.getDataSourceName(), jdbcTemplate);
		}
		return jdbcTemplate;
	}

	/**
	 * 通过名称获取数据元的jdbctemplate
	 * 
	 * @param dataSourceName
	 * @return
	 */
	public JdbcTemplate getJdbcTemplateFromDataSourceName(String dataSourceName) {
		// 获取数据元
		DataSourceMetaData dataSource = dataSourceService.getDataSource(dataSourceName);
		// 获取jdbctemplate
		JdbcTemplate jdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplate(dataSource);
		return jdbcTemplate;
	}

	/**
	 * 创建数据元Jdbctemplate
	 * 
	 * @param dataSource
	 * @return
	 */
	public static JdbcTemplate buildJdbcTemplate(DataSourceMetaData dataSource) {
		DataBaseTypeMetaData dbType = dataSource.getDataBaseType();
		BasicDataSource bds = new BasicDataSource();
		bds.setDriverClassName(dbType.getDriverClass());
		bds.setUrl(dataSource.getConnectionUrl());
		bds.setUsername(dataSource.getUserName());
		bds.setPassword(dataSource.getPassword());
		bds.setInitialSize(SyncConstant.JDBC_CONNECTIONS_INITIAL_SIZE); // 初始化连接数量
		bds.setMaxActive(SyncConstant.JDBC_CONNECTIONS_MAX_ACTIVE); // 最大链接数量
		bds.setMinIdle(SyncConstant.JDBC_CONNECTIONS_MIN_IDLE); // 最小空闲链接
		bds.setMaxIdle(SyncConstant.JDBC_CONNECTIONS_MAX_IDLE); // 最大空闲链接
		//bds.setDefaultAutoCommit(false);

		String typeDatabase = dataSource.getDataBaseType().toString();
		if (!typeDatabase.equals("HIVE")) {
			// bds.setMinIdle(0); // 最小空闲链接
			bds.setValidationQuery("select 1");
		}
		// else {
		// bds.setMaxWait(60 * 5); // 最大超时时间，以秒为单位
		// bds.setRemoveAbandonedTimeout(1000 * 60);// 超时时间，以毫秒为单位
		// }
		JdbcTemplate jdbcTemplate = new JdbcTemplate(bds);
		return jdbcTemplate;
	}
}