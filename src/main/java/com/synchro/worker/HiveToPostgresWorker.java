package com.synchro.worker;

import com.google.common.base.Joiner;
import com.synchro.common.constant.DatabaseType;
import com.synchro.common.constant.SyncConstant;
import com.synchro.dal.condition.TableMetaDataCondition;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;
import com.synchro.io.reader.HiveReader;
import com.synchro.io.writer.PostgresWriter;
import com.synchro.util.SpringContextUtils;
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
 * 从hive同步数据到postgresql
 *
 * @author liqiu
 */
public class HiveToPostgresWorker extends BaseWorker {

    private final static Logger LOGGER = LoggerFactory.getLogger(HiveToPostgresWorker.class);

    private final static Joiner joiner = Joiner.on(",");



    @Override
    public void init() {
        super.init();
        // 初始化线程池
        this.threadPool = Executors.newFixedThreadPool(SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE); // 线程池
        this.cyclicBarrier = new CyclicBarrier(SyncConstant.PG_TO_HIVE_THREAD_POOL_SIZE);


    }

    @Override
    protected void initQueue() {
        this.queue = new LinkedBlockingQueue<RowData>(SyncConstant.QUEUE_SIZE); // 初始化队列
    }

    @Override
    public void initSrcTableMetaData() {
        TableMetaDataCondition tableMetaDataCondition =
                new TableMetaDataCondition(options.getSrcDataSourceName(),
                        options.getSrcSchemaName(), options.getColumns(), options.getSrcTableName(), options.getWhere());

        this.srcTableMetaData = tableMetaDataService.getTableMetaData(tableMetaDataCondition, DatabaseType.HIVE);
    }

    @Override
    public void initTgtTableMetaData() {
        TableMetaDataCondition tableMetaDataCondition =
                new TableMetaDataCondition(options.getTgtDataSourceName(),
                        options.getTgtSchemaName(), options.getColumns(), options.getTgtTableName(), options.getWhere());

        this.tgtTableMetaData = tableMetaDataService.getTableMetaData(tableMetaDataCondition, DatabaseType.POSTGRES);
        if (tgtTableMetaData == null || !srcTableMetaData.equals(tgtTableMetaData)) {
            this.compareTableMetaData();
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

    @Override
    public void execute() {

        // 以目标数据表的字段信息为主
        srcTableMetaData.setColumnMetaDatas(tgtTableMetaData.getColumnMetaDatas());

		/* 删除目标数据 */
        this.delTgtData();

		/* 同步数据 */
        try {

            // 同步数据
            String sql = tableMetaDataService.getColumnSql(srcTableMetaData, options);
            LOGGER.info("get src Sql: " + sql);
            HiveReader hiveReader = new HiveReader(srcJdbcTemplate, sql, srcTableMetaData.getColumnMetaDatas(), queue, cyclicBarrier, isSrcRunning);
            Future<Boolean> hiveReaderFuture = threadPool.submit(hiveReader); // 与execute相比，线程执行完成以后可以通过引用获取返回值
            List<Future<Boolean>> postgresWriterFutureList = new ArrayList<Future<Boolean>>();// 消费者是否正常返回标志
            for (int i = 1; i < SyncConstant.THREAD_POOL_SIZE; i++) {
                PostgresWriter submitDataToDatabase = new PostgresWriter(tgtJdbcTemplate, tgtTableMetaData, queue, cyclicBarrier, isSrcRunning, isTgtRunning);
                postgresWriterFutureList.add(threadPool.submit(submitDataToDatabase));
            }

            // 判断返回值
            if (!hiveReaderFuture.get()) {
                LOGGER.error("get-data and submit data result is false");
            }
            for (Future<Boolean> submitDataToDatabaseFuture : postgresWriterFutureList) {
                if (!submitDataToDatabaseFuture.get()) {
                    LOGGER.error("get-data and submit data result is false");
                }
            }

            LOGGER.info("同步完成");
        } catch (Exception e) {
            LOGGER.error("get-data and submit data error" ,e);
        }
    }


    /**
     * 删除目标数据
     */
    public void delTgtData() {
        String delSql = "DELETE FROM " + options.getTgtSchemaName() + "." + options.getTgtTableName();
        if (options.getWhere() != null && !options.getWhere().equals("")) {
            delSql = delSql + (" where " + options.getWhere());
        }
        LOGGER.info("删除语句：" + delSql);
        tgtJdbcTemplate.execute(delSql.toString());
    }

    public static void main(String[] args) {

        SyncOptionsDto syncOptions = new SyncOptionsDto();
        syncOptions.setSrcDataSourceName("hive");
        syncOptions.setSrcSchemaName("business_mirror");
        syncOptions.setSrcTableName("b2c_product_ticket_price");
        syncOptions.setTgtDataSourceName("log_analysis");
        syncOptions.setTgtSchemaName("mirror");
        syncOptions.setTgtTableName("b2c_product_ticket_price");

        BaseWorker baseWorker = SpringContextUtils.getBean(HiveToPostgresWorker.class);
        baseWorker.setOptions(syncOptions);
        baseWorker.run();
    }
}