package com.synchro.worker;

import com.google.common.base.Joiner;
import com.synchro.common.constant.DatabaseType;
import com.synchro.common.constant.SyncConstant;
import com.synchro.dal.condition.TableMetaDataCondition;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;
import com.synchro.io.reader.PostgresReader;
import com.synchro.io.writer.PostgresWriter;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by xingxing.duan & liqiu on 2015/8/23.
 * Last modified by liqiu 2015-09-05
 */
public class PostgresToPostgresWorker extends BaseWorker {

    private final static Logger LOGGER = LoggerFactory.getLogger(PostgresToPostgresWorker.class);

    private final static Joiner joiner = Joiner.on(",");

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
        //临时解决方案
        try {
            this.tgtTableMetaData = tableMetaDataService.getTableMetaData(tableMetaDataCondition, DatabaseType.POSTGRES);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }

        if (tgtTableMetaData == null || !srcTableMetaData.equals(tgtTableMetaData)) {
            throw new RuntimeException("src table is not equals src table");
            /*
            this.compareAndCreateTableMetaData();
            //表创建后再次获取目标表元数据
            this.tgtTableMetaData = tableMetaDataService.getTableMetaData(tableMetaDataCondition, DatabaseType.POSTGRES);
            */
        }

    }

    private void compareAndCreateTableMetaData() {

        if (this.srcTableMetaData == null) {
            throw new RuntimeException("src table meta is null");
        }

        if (this.tgtTableMetaData == null) {
            LOGGER.info(" tgt table is not exists,we will create now");
            this.createTable();
            return;
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
            LOGGER.info("src table meta {} not equal tgt meta {} ,we will drop and create now", joiner.join(srcColumnMetaDatas), joiner.join(tgtColumnMetaDatas));
            this.dropTable();
            this.createTable();
        }

    }

    private void dropTable() {
        StringBuilder deleteStr = new StringBuilder();
        deleteStr.append(" drop table ").append(this.getOptions().getTgtSchemaName()).append(".").append(this.getOptions().getTgtTableName());
        this.tgtJdbcTemplate.execute(deleteStr.toString());
    }


    private void createTable() {
        StringBuilder columnStrBuff = new StringBuilder();

        columnStrBuff.append(" CREATE TABLE ").append(this.getOptions().getTgtSchemaName()).append(".").append(this.options.getTgtTableName()).append(" (");

        for (int i = 0; i < this.srcTableMetaData.getColumnMetaDatas().size(); i++) {
            if (i == 0) {
                columnStrBuff.append(this.srcTableMetaData.getColumnMetaDatas().get(i).getName());
                columnStrBuff.append(" ");
                columnStrBuff.append(this.srcTableMetaData.getColumnMetaDatas().get(i).getTypeName());
            } else {
                columnStrBuff.append(",");
                columnStrBuff.append(this.srcTableMetaData.getColumnMetaDatas().get(i).getName());
                columnStrBuff.append(" ");
                columnStrBuff.append(this.srcTableMetaData.getColumnMetaDatas().get(i).getTypeName());
            }
        }
        columnStrBuff.append(" ) ");
        this.tgtJdbcTemplate.execute(columnStrBuff.toString());
    }

    @Override
    public void execute() {

        this.delTgtData();

        // 生产者
        PostgresReader postgresReader = new PostgresReader(srcJdbcTemplate, srcTableMetaData, queue, isSrcRunning, isTgtRunning, options, this.cyclicBarrier);
        Future<Boolean> getDataFromExtractionFuture = threadPool.submit(postgresReader);

        // 消费者
        List<Future<Boolean>> submitDataToDatabaseFutureList = new ArrayList<Future<Boolean>>(); // 消费者是否正常返回标志
        for (int i = 1; i < SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE; i++) {
            PostgresWriter submitDataToDatabase = new PostgresWriter(tgtJdbcTemplate, tgtTableMetaData, queue, cyclicBarrier, isSrcRunning, isTgtRunning);
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

        } catch (Exception e) {
            LOGGER.error("[%s]:[%s]:[%s] get-data and submit data error", e);
        } finally {
            threadPool.shutdown();
        }

        LOGGER.info("同步完成");

    }

    /**
     * 删除目标数据
     */
    public void delTgtData() {
        String delSql = "DELETE FROM " + options.getTgtSchemaName() + "." + options.getTgtTableName();
        if (options.getWhere() != null && !options.getWhere().equals("")) {
            delSql = delSql + (" where " + options.getWhere());
        }
        tgtJdbcTemplate.execute(delSql.toString());
    }

    public SyncOptionsDto getOptions() {
        return options;
    }

    public void setOptions(SyncOptionsDto options) {
        this.options = options;
    }

    public static void main(String[] args) {

        SyncOptionsDto syncOptions = new SyncOptionsDto();
        syncOptions.setSrcDataSourceName("log_analysis");
        syncOptions.setSrcSchemaName("mirror");
        syncOptions.setSrcTableName("sp_product");
        syncOptions.setTgtDataSourceName("log_analysis");
        syncOptions.setTgtSchemaName("realtime_data");
        syncOptions.setTgtTableName("sp_product");
        syncOptions.setSplitByColumn("id");
        syncOptions.setDirect(true);

        PostgresToPostgresWorker baseWorker = new PostgresToPostgresWorker();
        baseWorker.setOptions(syncOptions);
        baseWorker.run();
    }
}
