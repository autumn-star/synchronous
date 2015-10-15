package com.synchro.worker;

import com.synchro.common.constant.DatabaseType;
import com.synchro.common.constant.SyncConstant;
import com.synchro.dal.condition.TableMetaDataCondition;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.dal.metadata.RowData;
import com.synchro.io.reader.PostgresReader;
import com.synchro.io.writer.HiveWriter;
import com.synchro.util.PropertiesUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by xingxing.duan & liqiu on 2015/8/23.
 * Last modified by liqiu 2015-09-05
 */
public class PostgresToHiveWorker extends BaseWorker {

	private final static Logger LOGGER = LoggerFactory.getLogger(PostgresToHiveWorker.class);

	String tempFileFold;

	@Override
	public void init() {

		super.init();
		// 初始化线程池
		this.threadPool = Executors.newFixedThreadPool(SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE); // 线程池
		this.cyclicBarrier = new CyclicBarrier(SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE);

		this.tempFileFold = PropertiesUtils.getProperties("tempFileFold");
	}

	@Override
	protected void initQueue() {
		this.queue = new LinkedBlockingQueue<RowData>(SyncConstant.PG_TO_HIVE_QUEUE_SIZE); // 初始化队列
	}

	@Override
	public void initSrcTableMetaData() {

		TableMetaDataCondition metaDataCondition = new TableMetaDataCondition(options.getSrcDataSourceName(), options.getSrcSchemaName(), options.getColumns(), options.getExcludeColumns(),
				options.getSrcTableName(), options.getWhere());
		this.srcTableMetaData = tableMetaDataService.getTableMetaData(metaDataCondition, DatabaseType.POSTGRES);
	}

	@Override
	public void initTgtTableMetaData() {

		TableMetaDataCondition metaDataCondition = new TableMetaDataCondition(options.getTgtDataSourceName(), options.getTgtSchemaName(), options.getColumns(), options.getTgtTableName(),
				options.getWhere());

		this.tgtTableMetaData = this.tableMetaDataService.getTableMetaData(metaDataCondition, DatabaseType.HIVE);
		if (tgtTableMetaData == null) {
			// 数据表不存在，重新创建
			this.tgtTableMetaData = metaDataCondition.convert();
			this.tgtTableMetaData.setColumnMetaDatas(this.srcTableMetaData.getColumnMetaDatas());
			hiveDao.createTable(tgtJdbcTemplate, tgtTableMetaData);
			this.tgtTableMetaData = this.tableMetaDataService.getTableMetaData(metaDataCondition, DatabaseType.HIVE); // 重新赋值

		} else {
			// 获取数据表信息
			LOGGER.info("获取数据表信息");
			tgtTableMetaData.setColumnMetaDatas(hiveDao.getColumnMetaData(tgtJdbcTemplate, options.getTgtSchemaName(), options.getTgtTableName()));
			// 查看数据是否完整
			if (!srcTableMetaData.equals(tgtTableMetaData)) {
				// 如果不相符，创建一个新的数据表
				LOGGER.warn(options.getTgtSchemaName() + options.getTgtTableName() + "table not the same! srcTableMetaData:" + srcTableMetaData + ";tgtTableMetaData:" + tgtTableMetaData);
				hiveDao.dropTable(tgtJdbcTemplate, tgtTableMetaData);
				tgtTableMetaData.setColumnMetaDatas(srcTableMetaData.getColumnMetaDatas());
				hiveDao.createTable(tgtJdbcTemplate, tgtTableMetaData);
			} else {
				// 删除数据
				if (StringUtils.isBlank(options.getPartitionColumnValue())) {
					hiveDao.truncatTable(tgtJdbcTemplate, tgtTableMetaData);
				} else {
					hiveDao.dropTablePartition(tgtJdbcTemplate, tgtTableMetaData, options.getPartitionColumnValue());
				}
			}
		}

	}

	/**
	 * 从postgresql同步数据到hive
	 *
	 * @param
	 * @return
	 */
	@Override
	public void execute() {

		// 生产者
		PostgresReader postgresReader = new PostgresReader(srcJdbcTemplate, srcTableMetaData, queue, isSrcRunning, isTgtRunning, options, this.cyclicBarrier);
		Future<Boolean> getDataFromExtractionFuture = threadPool.submit(postgresReader);

		// 消费者
		List<Future<Boolean>> submitDataToDatabaseFutureList = new ArrayList<Future<Boolean>>(); // 消费者是否正常返回标志
		for (int i = 1; i < SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE; i++) {
			HiveWriter submitDataToDatabase = new HiveWriter(tgtTableMetaData, queue, isSrcRunning, isTgtRunning, options, tempFileFold, hiveDao, this.cyclicBarrier);
			submitDataToDatabaseFutureList.add(threadPool.submit(submitDataToDatabase));
		}

		// 返回值
		try {

			if (!getDataFromExtractionFuture.get()) {
				LOGGER.error("get-data result is false");
			} else {
				LOGGER.info("get-data result is true");
			}
			for (Future<Boolean> submitDataToDatabaseFuture : submitDataToDatabaseFutureList) {
				if (!submitDataToDatabaseFuture.get()) {
					LOGGER.error("submit data result is false");
				} else {
					LOGGER.info("submit data result is true");
				}
			}

		} catch (Exception e) {
			LOGGER.error("[%s]:[%s]:[%s] get-data and submit data error",e);
		}

		LOGGER.info("同步完成");
	}

	public SyncOptionsDto getOptions() {
		return options;
	}

	public void setOptions(SyncOptionsDto options) {
		this.options = options;
	}

	public static void main(String[] args) {

		SyncOptionsDto syncOptions = new SyncOptionsDto();
		syncOptions.setSrcDataSourceName("src_tts_online");
		syncOptions.setSrcSchemaName("public");
		syncOptions.setSrcTableName("b2c_product_ticket_price");
		syncOptions.setTgtDataSourceName("hive");
		syncOptions.setTgtSchemaName("business_mirror");
		syncOptions.setTgtTableName("b2c_product_ticket_price");
		syncOptions.setSplitByColumn("id");
		StringBuilder sql = new StringBuilder();
		sql.append("select min(").append(syncOptions.getSplitByColumn()).append(") as min, max(").append(syncOptions.getSplitByColumn()).append(") as max");
		sql.append(" from ").append(syncOptions.getSrcSchemaName()).append(".").append(syncOptions.getSrcTableName());
		// 按条件同步
		if (StringUtils.isNotBlank(syncOptions.getWhere())) {
			sql.append(" where " + syncOptions.getWhere());
		}

		System.out.print(sql);
	}
}