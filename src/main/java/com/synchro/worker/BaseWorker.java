package com.synchro.worker;

import com.synchro.dal.dao.HiveDao;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.dal.libs.JdbcTemplateFactory;
import com.synchro.dal.metadata.RowData;
import com.synchro.dal.metadata.TableMetaData;
import com.synchro.service.TableMetaDataService;
import com.synchro.util.SpringContextUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by xingxing.duan on 2015/8/23. basic worker that use reader and write
 * Last modified by liqiu 2015-09-05 Sat.
 */
public abstract class BaseWorker {

	protected SyncOptionsDto options;

	protected JdbcTemplate srcJdbcTemplate;
	protected JdbcTemplate tgtJdbcTemplate;

	protected TableMetaData srcTableMetaData;
	protected TableMetaData tgtTableMetaData;

	protected ExecutorService threadPool;
	protected LinkedBlockingQueue<RowData> queue;

	protected CyclicBarrier cyclicBarrier;

	protected AtomicBoolean isSrcRunning;
	protected AtomicBoolean isTgtRunning;

	TableMetaDataService tableMetaDataService;

	HiveDao hiveDao;


	/**
	 * before worker work
	 */
	protected void init() {

		this.srcJdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplateFromDataSourceName(options.getSrcDataSourceName());
		this.tgtJdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplateFromDataSourceName(options.getTgtDataSourceName());

		this.isSrcRunning = new AtomicBoolean(); // 源数据是否启动
		this.isTgtRunning = new AtomicBoolean(); // 目标数据是否启动

		this.tableMetaDataService = SpringContextUtils.getBean(TableMetaDataService.class);
		this.hiveDao = SpringContextUtils.getBean(HiveDao.class);

		this.initSrcTableMetaData();
		this.initTgtTableMetaData();
		if (options.getQueueSize() > 0) {
			this.queue = new LinkedBlockingQueue<RowData>(options.getQueueSize()); // 初始化队列
		} else {
			initQueue();
		}
	}

	protected abstract void initQueue();

	protected abstract void initSrcTableMetaData();

	protected abstract void initTgtTableMetaData();

	protected abstract void execute();

	public void run() {
		init(); // 初始化
		execute(); // 执行
		clear(); // 清理
	}

	/**
	 * clear all param after transformData
	 */
	protected void clear() {
		this.srcJdbcTemplate = null;
		this.tgtJdbcTemplate = null;

		threadPool.shutdown();
		threadPool = null;
		cyclicBarrier = null;
	}

	public void setOptions(SyncOptionsDto options) {
		this.options = options;
	}

	public JdbcTemplate getSrcJdbcTemplate() {
		return srcJdbcTemplate;
	}

}
