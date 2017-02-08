package com.synchro.io.reader;

import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;
import com.synchro.common.constant.SyncConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 生产者，从Hive获取数据
 * 
 * @author qiu.li
 */
public class HiveReader implements Callable<Boolean> {

	private static final Logger logger = LoggerFactory.getLogger(HiveReader.class);
	private final LinkedBlockingQueue<RowData> queue; // 队列
	private AtomicBoolean isRunning; // 是否开始跑数据
	private String sql; //
	private final List<ColumnMetaData> columnMetaDataList; // 字段列表
	private CyclicBarrier cyclicBarrier; // 开始的标志
	private JdbcTemplate jdbcTemplate;

	public HiveReader(JdbcTemplate jdbcTemplate, String sql, List<ColumnMetaData> columnMetaDataList, LinkedBlockingQueue<RowData> queue, CyclicBarrier cyclicBarrier, AtomicBoolean isRunning) {
		this.jdbcTemplate = jdbcTemplate;
		this.sql = sql;
		this.columnMetaDataList = columnMetaDataList;
		this.queue = queue;
		this.cyclicBarrier = cyclicBarrier;
		this.isRunning = isRunning;
	}

	@Override
	public Boolean call() throws Exception {
		boolean result = false;
		try {
			// 将数据放入队列
			this.getDataAndPutToQueue();
			result = true;
		} catch (Exception e) {
			result = false;
			e.printStackTrace();
			throw e;
		}
		return result;
	}

	/**
	 * 从hive获取数据并放入队列
	 * 
	 * @throws Exception
	 */
	private void getDataAndPutToQueue() throws Exception {
		long beginTime = System.currentTimeMillis();
		this.isRunning.set(true);
		cyclicBarrier.await();
		try {
			jdbcTemplate.execute("set hive.cli.print.row.to.vertical=true;");
			jdbcTemplate.queryForObject(sql, new RowMapper<ResultSet>() {
				@Override
				public ResultSet mapRow(ResultSet rs, int rowNum) throws SQLException {
					// 保留第一条
					do {
						putData(rs, rowNum);
					} while (rs.next());

					return rs;
				}
			});
			logger.info(String.format("Time cost of getDataAndPutToQueue: %s seconds", (System.currentTimeMillis() - beginTime) / 1000.0));
		} catch (EmptyResultDataAccessException emptyResultException) {
			if (queue.size() > 1) {
				logger.info("Read hive data finished: data size " + queue.size());
			} else {
				throw emptyResultException;
			}
		} catch (Exception e) {
			logger.error(e.toString());
			throw e;
		}

		this.isRunning.set(false);
	}

	public AtomicBoolean isRunning() {
		return this.isRunning;
	}

	private void putData(ResultSet rs, int rowNum) throws SQLException {
		Object[] columnObjs = new String[columnMetaDataList.size()];
		int i = 0;
		for (ColumnMetaData t : columnMetaDataList) {
			columnObjs[i++] = rs.getString(t.getName());
		}

		RowData rowData = new RowData(columnObjs);
		try {
			queue.put(rowData);
		} catch (InterruptedException e) {
			logger.info(e.toString());
		}
		// logger.info("rowData:" + rowData.toString());
		if (rowNum > 0 && rowNum % SyncConstant.LOGGER_SIZE == 0) {
			logger.info(" put data to queue line num: " + rowNum + "; queue size: " + queue.size());
		}
	}
}
