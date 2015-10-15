package com.synchro.io.writer;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.synchro.common.constant.SyncConstant;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;
import com.synchro.dal.metadata.TableMetaData;
import com.synchro.service.ColumnAdapterService;
import com.synchro.service.SqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费者，将队列内的数据保存到Database
 *
 * @author qiu.li
 */
public class RealtimePostgresWriter implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(RealtimePostgresWriter.class);

    private TableMetaData tableMetaData;

    private LinkedBlockingQueue<RowData> queue;

    private AtomicBoolean isGetDataRunning;
    private AtomicBoolean isRunning;

    private CyclicBarrier cyclicBarrier;
    private List<ColumnMetaData> columnMetaDataList;

    private JdbcTemplate jdbcTemplate;

    private int primaryKeyIndex;

    private boolean deleteBeforeInsert =false;

    public RealtimePostgresWriter(JdbcTemplate jdbcTemplate, TableMetaData tableMetaData, LinkedBlockingQueue<RowData> dataQueue,
                                  CyclicBarrier cyclicBarrier, AtomicBoolean isGetDataRunning, AtomicBoolean isRunning,int primaryKeyIndex,boolean deleteBeforeInsert) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableMetaData = tableMetaData;
        this.columnMetaDataList = tableMetaData.getColumnMetaDatas();
        this.queue = dataQueue;
        this.cyclicBarrier = cyclicBarrier;
        this.isGetDataRunning = isGetDataRunning;
        this.isRunning = isRunning;
        this.primaryKeyIndex = primaryKeyIndex;
        this.deleteBeforeInsert = deleteBeforeInsert;
    }

    @Override
    public Boolean call() {
        long beginTime = System.currentTimeMillis();
        this.isRunning.set(true);
        try {
            cyclicBarrier.await();
            int lineNum = 0;
            int commitCount = 0; // 缓存数量
            List<RowData> tmpRowDataList = new ArrayList<RowData>();// 缓存数组
            while (this.isGetDataRunning.get() || this.queue.size() > 0) {
                // 从队列获取一条数据
                RowData rowData = this.queue.poll(1, TimeUnit.SECONDS);
                if (rowData == null) {
                    logger.info("this.isGetDataRunning:" + this.isGetDataRunning + ";this.queue.size():" + this.queue.size());
                    Thread.sleep(10000);
                    continue;
                }
                // 添加到缓存数组
                tmpRowDataList.add(rowData);
                lineNum++;
                commitCount++;
                if (commitCount == SyncConstant.INSERT_SIZE) {
                    this.insertContractAch(tmpRowDataList); // 批量写入
                    tmpRowDataList.clear(); // 清空缓存
                    commitCount = 0;
                }

                if (lineNum % SyncConstant.LOGGER_SIZE == 0) {
                    logger.info(" commit line: " + lineNum + "; queue size: " + queue.size());
                }
            }

            this.insertContractAch(tmpRowDataList); // 批量写入
            tmpRowDataList.clear();// 清空缓存
            logger.info(" commit line end: " + lineNum);
        } catch (Exception e) {
            logger.error(" submit data error" , e);
        } finally {
            this.isRunning.set(false);
        }
        logger.info(String.format("SubmitDataToDatabase used %s second times", (System.currentTimeMillis() - beginTime) / 1000.00));
        return true;
    }

    /**
     * 批量插入数据
     *
     * @param rowDatas
     * @return
     */
    public int insertContractAch(List<RowData> rowDatas) {
        final List<RowData> tmpObjects = rowDatas;
        if(deleteBeforeInsert){
            deleteContractAch(rowDatas);
        }
        String sql = SqlService.createInsertPreparedSql(tableMetaData); // 获取sql
        try {
            int[] index = this.jdbcTemplate.batchUpdate(sql, new PreparedStatementSetter(tmpObjects, this.columnMetaDataList));
            return index.length;
        } catch (Exception e) {
            logger.error(" insertContractAch error: " + e.toString());
        }
        return 0;
    }

    /**
     * 批量删除
     * @param rowDatas
     */
    private void  deleteContractAch(List<RowData> rowDatas){
        final List<Integer> primaryKeys=Lists.transform(rowDatas, new Function<RowData, Integer>() {
            @Override
            public Integer apply(RowData input) {
                return (Integer) input.getColumnObjects()[primaryKeyIndex];
            }
        });
        String deleteSql = SqlService.createDeleteSql(tableMetaData);
        jdbcTemplate.batchUpdate(deleteSql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setInt(1,primaryKeys.get(i));
            }

            @Override
            public int getBatchSize() {
                return primaryKeys.size();
            }
        });
    }

    /**
     * 处理批量插入的回调类
     */
    private class PreparedStatementSetter implements BatchPreparedStatementSetter {
        private List<RowData> rowDatas;
        private List<ColumnMetaData> columnMetaDataList;

        /**
         * 通过构造函数把要插入的数据传递进来处理
         */
        public PreparedStatementSetter(List<RowData> rowDatas, List<ColumnMetaData> columnList) {
            this.rowDatas = rowDatas;
            this.columnMetaDataList = columnList;
        }

        public int getBatchSize() {
            return this.rowDatas.size();
        }

        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            RowData rowData = this.rowDatas.get(i);
            for (int j = 0; j < rowData.getColumnObjects().length; j++) {
                // 类型转换
                try {
                    ColumnAdapterService.setParameterValueWithPg(ps, j + 1, rowData.getColumnObjects()[j], this.columnMetaDataList.get(j).getType());
                } catch (Exception e) {
                    ps.setObject(j + 1, null);
                    logger.error("value error: " + e);
                    logger.error("columnMetaDataList:" + columnMetaDataList);
                }
            }
        }
    }

    public AtomicBoolean isRunning() {
        return this.isRunning;
    }

}