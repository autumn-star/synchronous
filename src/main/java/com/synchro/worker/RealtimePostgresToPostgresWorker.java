package com.synchro.worker;

import com.google.common.base.Joiner;
import com.synchro.common.constant.DatabaseType;
import com.synchro.common.constant.SyncConstant;
import com.synchro.dal.condition.TableMetaDataCondition;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;
import com.synchro.io.reader.PostgresReader;
import com.synchro.io.writer.RealtimePostgresWriter;
import com.synchro.util.DateUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by xingxing.duan on 2015/9/16.
 * 实时同步工具类
 */
public class RealtimePostgresToPostgresWorker extends BaseWorker {
    private final static Logger LOGGER = LoggerFactory.getLogger(PostgresToPostgresWorker.class);

    private final static Joiner joiner = Joiner.on(",");

    private static final String PRIMARY_KEY = "id";
    //休息10秒
    private static final long SLEEP_TIME = 10;

    private int primaryKeyIndex;

    @Override
    public void init() {

        // 初始化线程池
        super.init();
        this.threadPool = Executors.newFixedThreadPool(SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE); // 线程池
        this.cyclicBarrier = new CyclicBarrier(SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE);
    }

    @Override
    protected void initQueue() {
        this.queue = new LinkedBlockingQueue<RowData>(SyncConstant.QUEUE_SIZE); // 初始化队列
    }

    @Override
    public void initSrcTableMetaData() {
        TableMetaDataCondition tableMetaDataCondition = new TableMetaDataCondition(options.getSrcDataSourceName(), options.getSrcSchemaName(), options.getColumns(), options.getSrcTableName(),
                options.getWhere());
        this.srcTableMetaData = tableMetaDataService.getTableMetaData(tableMetaDataCondition, DatabaseType.POSTGRES);
    }

    @Override
    public void initTgtTableMetaData() {
        TableMetaDataCondition tableMetaDataCondition = new TableMetaDataCondition(options.getTgtDataSourceName(), options.getTgtSchemaName(), options.getColumns(), options.getTgtTableName(),
                options.getWhere());

        this.tgtTableMetaData = tableMetaDataService.getTableMetaData(tableMetaDataCondition, DatabaseType.POSTGRES);
        if (tgtTableMetaData == null || !srcTableMetaData.equals(tgtTableMetaData)) {
            this.compareTableMetaData();
        }
    }

    @Override
    public void execute() {

        this.initPrimaryKeyIndex();

        while(true){
            // 生产者

            //组装where条件
            this.buildWhereCondition();

            PostgresReader postgresReader = new PostgresReader(srcJdbcTemplate, srcTableMetaData, queue, isSrcRunning, isTgtRunning, options, this.cyclicBarrier);
            Future<Boolean> getDataFromExtractionFuture = threadPool.submit(postgresReader);

            // 消费者
            List<Future<Boolean>> submitDataToDatabaseFutureList = new ArrayList<Future<Boolean>>(); // 消费者是否正常返回标志
            for (int i = 1; i < SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE; i++) {
                RealtimePostgresWriter submitDataToDatabase = new RealtimePostgresWriter(tgtJdbcTemplate, tgtTableMetaData, queue, cyclicBarrier, isSrcRunning, isTgtRunning, primaryKeyIndex, true);
                submitDataToDatabaseFutureList.add(threadPool.submit(submitDataToDatabase));
            }

            // 返回值
            try {
                for (Future<Boolean> submitDataToDatabaseFuture : submitDataToDatabaseFutureList) {
                    if (!submitDataToDatabaseFuture.get()) {
                        LOGGER.error("get-data and submit data result is false");
                    }
                }

                if (!getDataFromExtractionFuture.get()) {
                    LOGGER.error("get-data and submit data result is false");
                }
                TimeUnit.SECONDS.sleep(SLEEP_TIME);
                LOGGER.info("sleep end");
            } catch (Exception e) {
                LOGGER.error("[%s]:[%s]:[%s] get-data and submit data error",e);
            }

        }

    }

    private void compareTableMetaData() {

        if (this.srcTableMetaData == null) {
            throw new RuntimeException("src table meta is null");
        }

        if (this.tgtTableMetaData == null) {
            throw new RuntimeException("tgt table meta is null");
        }
        List<ColumnMetaData> srcColumnMetaDatas = srcTableMetaData.getColumnMetaDatas();
        List<ColumnMetaData> tgtColumnMetaDatas = tgtTableMetaData.getColumnMetaDatas();


        Iterator<ColumnMetaData> metaDataIterator = srcColumnMetaDatas.iterator();
        while (metaDataIterator.hasNext()) {
            ColumnMetaData rowData = metaDataIterator.next();
            int index = tgtColumnMetaDatas.indexOf(rowData);
            if (index > -1) {
                metaDataIterator.remove();
                tgtColumnMetaDatas.remove(index);
            }
        }

        if (CollectionUtils.isNotEmpty(srcColumnMetaDatas) || CollectionUtils.isNotEmpty(tgtColumnMetaDatas)) {
            String errorInfo = String.format("src table meta [%s] not equal tgt meta [%s]", joiner.join(srcColumnMetaDatas), joiner.join(tgtColumnMetaDatas));
            throw new RuntimeException(errorInfo);
        }

    }

    private void initPrimaryKeyIndex() {
        List<ColumnMetaData> columnMetaDatas = this.tgtTableMetaData.getColumnMetaDatas();
        this.primaryKeyIndex = columnMetaDatas.indexOf(new ColumnMetaData(PRIMARY_KEY));
        if (this.primaryKeyIndex == -1) {
            throw new RuntimeException("只支持主键为id的情况");
        }
    }

    private void buildWhereCondition(){

        String partitionColumnName = this.options.getPartitionColumnName();
        List<ColumnMetaData> columnMetaDatas = this.tgtTableMetaData.getColumnMetaDatas();
        int partitionIndex = columnMetaDatas.indexOf(new ColumnMetaData(partitionColumnName));
        ColumnMetaData columnMetaData=columnMetaDatas.get(partitionIndex);
        StringBuilder sql = new StringBuilder();
        sql.append("select ").append(" max(").append(partitionColumnName).append(") as max");
        sql.append(" from ").append(options.getTgtSchemaName()).append(".").append(options.getTgtTableName());
        Map<String, Object> map = tgtJdbcTemplate.queryForMap(sql.toString());
        Object max = map.get("max");
        String where;
        switch (columnMetaData.getType()) {

            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIGINT:
                where = partitionColumnName+" > "+max;
                options.setWhere(where);
                return;

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                where = partitionColumnName+" > '"+ DateUtils.dateToString((Date) max,DateUtils.DATE_FORMAT_yyyyMMdd_HHmmssSSSZ)+"'";
                options.setWhere(where);
                return;
        }

    }

    public SyncOptionsDto getOptions() {
        return options;
    }

    public void setOptions(SyncOptionsDto options) {
        this.options = options;
    }
}
