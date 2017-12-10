package com.synchro.io.reader;

import com.synchro.common.constant.SyncConstant;
import com.synchro.dal.libs.JdbcTemplateFactory;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;
import com.synchro.dal.metadata.TableMetaData;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.service.copy.CopyService;
import com.synchro.io.split.DateSplitter;
import com.synchro.io.split.InputSplit;
import com.synchro.io.split.InputSplitter;
import com.synchro.io.split.IntegerSplitter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a reader for postgres database
 *
 * @author xingxing
 * @CreateTime 2017-08-21
 */
public class PostgresReader implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReader.class);

    private JdbcTemplate jdbcTemplate;
    private TableMetaData tableMetaData;
    private LinkedBlockingQueue<RowData> queue;
    private AtomicBoolean isSrcRunning;
    private AtomicBoolean isTgtRunning;
    private SyncOptionsDto syncOptions;
    private CyclicBarrier cyclicBarrier;

    public PostgresReader(JdbcTemplate jdbcTemplate, TableMetaData tableMetaData, LinkedBlockingQueue<RowData> queue, AtomicBoolean isSrcRunning, AtomicBoolean isTgtRunning, SyncOptionsDto syncOptions,
                          CyclicBarrier cyclicBarrier) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableMetaData = tableMetaData;
        this.queue = queue;
        this.isSrcRunning = isSrcRunning;
        this.isTgtRunning = isTgtRunning;
        this.syncOptions = syncOptions;
        this.cyclicBarrier = cyclicBarrier;
    }

    @Override
    public Boolean call() throws Exception {
        boolean result;
        try {
            this.getDataAndPutToQueue();
            result = true;
        } catch (Exception e) {
            LOGGER.error("getDataAndPutToQueue faild", e);
            result = false;
        }
        return result;
    }

    /**
     * 从数据源取得数据并加入队列
     *
     * @throws Exception
     */
    private void getDataAndPutToQueue() throws Exception {
        LOGGER.info("PostgresReader start to put data to queue");
        this.isSrcRunning.set(true);
        cyclicBarrier.await();
        long beginTime = System.currentTimeMillis();
        String splitByColumn = syncOptions.getSplitByColumn();

        try {
            if (syncOptions.isDirect()) {
                // 直接copy模式
                CopyService copyService = new CopyService(queue, syncOptions, getSelectQuery(),tableMetaData.getColumnMetaDatas());
                LOGGER.info("PostgresReader use copy");
                copyService.putCopyData();
                LOGGER.info("PostgresReader copy end");
                return;
            } else {
                String selectQuery = getSelectQuery();
                //synchronous data for split column
                if (StringUtils.isNotBlank(splitByColumn)) {

                    List<InputSplit> inputSplits = getSplits();
                    for (InputSplit inputSplit : inputSplits) {
                        String partitionQuery = getPartitionQuery(selectQuery, inputSplit);
                        /*LOGGER.info(partitionQuery);*/
                        SqlRowSet sqlRowSet = queryForPartitionMode(partitionQuery);
                        putData(sqlRowSet);
                    }
                } else {
                    SqlRowSet sqlRowSet = queryForPartitionMode(selectQuery);
                    putData(sqlRowSet);
                }
            }
        } catch (Exception ex) {
            LOGGER.error(" submit data error" ,ex);
            throw ex;
        } finally {
            this.isSrcRunning.set(false);
        }

        LOGGER.info(String.format(" put data to queue ok, use %s second times", (System.currentTimeMillis() - beginTime) / 1000.00));
    }

    public List<InputSplit> getSplits() throws SQLException {
        JdbcTemplate srcJdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplateFromDataSourceName(syncOptions.getSrcDataSourceName());
        SqlRowSet sqlRowSet = srcJdbcTemplate.queryForRowSet(getBoundingValsQuery());
        sqlRowSet.next();
        int sqlDataType = sqlRowSet.getMetaData().getColumnType(1);
        InputSplitter inputSplitter = getSplitter(sqlDataType);
        List<InputSplit> inputSplits = inputSplitter.split(sqlRowSet, syncOptions.getSplitByColumn());
        return inputSplits;


    }

    public String getBoundingValsQuery() {

        StringBuilder sql = new StringBuilder();
        sql.append("select min(").append(syncOptions.getSplitByColumn()).append(") as min, max(").append(syncOptions.getSplitByColumn()).append(") as max");
        sql.append(" from ").append(syncOptions.getSrcSchemaName()).append(".").append(syncOptions.getSrcTableName());
        // 按条件同步
        if (StringUtils.isNotBlank(syncOptions.getWhere())) {
            sql.append(" where " + syncOptions.getWhere());
        }
        return sql.toString();
    }

    protected InputSplitter getSplitter(int sqlDataType) {
        switch (sqlDataType) {

            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIGINT:
                return new IntegerSplitter();

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return new DateSplitter();

            default:
                return null;
        }
    }

    public String getPartitionQuery(String selectQuery, InputSplit inputSplit) {
        StringBuilder partitionQuery = new StringBuilder(selectQuery);
        if(StringUtils.isBlank(syncOptions.getWhere())){
            partitionQuery.append(" where 1 = 1 and ");
        }else{
            partitionQuery.append("  and ");
        }
        partitionQuery.append(inputSplit.getLowerBoundClause()).append(" and ").append(inputSplit.getUpperBoundClause());
        return partitionQuery.toString();
    }

    public String getSelectQuery() {
        StringBuilder querySql = new StringBuilder();
        querySql.append("SELECT " + tableMetaData.getColumnsStr() + " FROM ").append(syncOptions.getSrcSchemaName()).append(".").append(syncOptions.getSrcTableName());

        if (StringUtils.isNotBlank(syncOptions.getWhere())) {
            querySql.append(" where " + syncOptions.getWhere());
        }
        return querySql.toString();
    }

    /**
     * 分区查询，失败后sleep ConstantInterface.PG_TO_HIVE_SELECT_FAILD_SLEEP_TIME后重试
     *
     * @param sql
     * @return
     */
    private SqlRowSet queryForPartitionMode(String sql) {
        jdbcTemplate.setFetchSize(100);
        try {
            return jdbcTemplate.queryForRowSet(sql);
        } catch (Exception ex) {
            LOGGER.error("{} 查询失败", sql, ex);
            try {
                Thread.sleep(SyncConstant.PG_TO_HIVE_SELECT_FAILD_SLEEP_TIME);
            } catch (InterruptedException e) {
                LOGGER.error("sleep 1 min error", ex);
            }
            return queryForPartitionMode(sql);
        }

    }

    /**
     * 向缓存队列插入数据
     *
     * @param resultSet
     * @throws Exception
     */
    private void putData(SqlRowSet resultSet) throws Exception {
        int rowNum = 0;
        while (resultSet.next()) {
            if (!this.isTgtRunning.get()) {
                LOGGER.error(" put data to queue need to stop, tgt-running is end(maybe error)");
                Exception e = new Exception();
                throw e;
            }
            Object[] columnObjs = new Object[this.tableMetaData.getColumnMetaDatas().size()];
            for (int i = 0; i < columnObjs.length; i++) {
                columnObjs[i] = ColumnMetaData.getColumnValue(resultSet, tableMetaData.getColumnMetaDatas().get(i));
            }
            RowData rowData = new RowData(columnObjs);
            this.queue.put(rowData);

            rowNum++;
            if (rowNum > 0 && rowNum % SyncConstant.LOGGER_SIZE == 0) {
                LOGGER.info(" put data to queue line num: " + rowNum + "; queue size: " + this.queue.size());
            }

        }
        LOGGER.info(" put data to queue line num(end): " + rowNum);
    }

    public boolean isRunning() {
        return this.isSrcRunning.get();
    }

}
