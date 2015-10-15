package com.synchro.io.writer;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.synchro.dal.dao.HiveDao;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;
import com.synchro.dal.metadata.TableMetaData;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.common.constant.SyncConstant;
import com.synchro.util.HdfsUtils;
import com.synchro.common.constant.HiveDivideConstant;
import com.synchro.util.HiveUtils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a writer for writer data to hive
 *
 * @author xingxing
 * @CreateTime 2015-08-21
 */
public class HiveWriter implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveWriter.class);

    private final static Joiner joiner = Joiner.on("\001").useForNull("\\N");

    private LinkedBlockingQueue<RowData> queue;

    private AtomicBoolean isGetDataRunning;
    private AtomicBoolean isRunning;

    private List<ColumnMetaData> columnMetaDataList;

    private SyncOptionsDto pgHiveDto;

    private String tempFileFold;

    private HiveDao hiveDao;

    private CyclicBarrier cyclicBarrier;

    public HiveWriter(TableMetaData tableMetaData, LinkedBlockingQueue<RowData> queue, AtomicBoolean isGetDataRunning, AtomicBoolean isRunning, SyncOptionsDto pgHiveDto, String tempFileFold, HiveDao hiveDao, CyclicBarrier cyclicBarrier) {

        this.columnMetaDataList = tableMetaData.getColumnMetaDatas();
        this.queue = queue;
        this.isGetDataRunning = isGetDataRunning;
        this.isRunning = isRunning;
        this.pgHiveDto = pgHiveDto;
        this.tempFileFold = tempFileFold;
        this.hiveDao = hiveDao;
        this.cyclicBarrier = cyclicBarrier;
    }

    @Override
    public Boolean call() throws Exception {
        long beginTime = System.currentTimeMillis();
        this.isRunning.set(true);
        FSDataOutputStream output = null;
        try {

            String tgtTableName = pgHiveDto.getTgtTableName();
            String dirParent = tempFileFold + tgtTableName; // 目录

            // UUID(Universally Unique Identifier) 通用唯一识别码
            Path dst = new Path(dirParent, UUID.randomUUID().toString());

            // 创建目录
            HdfsUtils.createDir(dirParent);
            // 创建文件
            output = HdfsUtils.hdfs.create(dst, false);

            int lineNum = 0;

            cyclicBarrier.await(); // 等待生产者一起行动

            // 添加到临时数据组
            StringBuilder sb = new StringBuilder();

            while (this.isGetDataRunning.get() || this.queue.size() > 0) {
                // 从队列获取一条数据
                RowData rowData = this.queue.poll(1, TimeUnit.SECONDS);

                if (rowData == null) {
                    LOGGER.info("this.isGetDataRunning:" + this.isGetDataRunning + ";this.queue.size():" + this.queue.size());
                    Thread.sleep(1000);
                    continue;
                }
                Object[] columnObjects = rowData.getColumnObjects();

                lineNum++;
                List<Object> rowList = Lists.newArrayList();
                for (int i = 0; i < columnMetaDataList.size(); i++) {
                    Object value = columnObjects[i];
                    if (value == null) {
                        rowList.add(value);
                    } else {
                        // 替换特殊字符，并添加
                        rowList.add(HiveUtils.replaceBlank(value.toString()));
                        // rowList.add(value.toString().replaceAll(HiveDivideInterface.LINE_DIVIDE, "").replaceAll(HiveDivideInterface.COLUMN_DIVIDE, "").replaceAll(HiveDivideInterface.HIVE_DIVIDE, ""));
                    }
                }
                sb.append(joiner.join(rowList)).append(HiveDivideConstant.ENTER);// 换行
                if (lineNum % SyncConstant.PG_TO_HIVE_LINE_SIZE == 0) {
                    byte[] buffer = sb.toString().getBytes("utf-8");
                    // LOGGER.info("大小 {}", buffer.length);
                    output.write(buffer);
                    output.flush();
                    sb.delete(0, sb.length());
                }

                if (lineNum % SyncConstant.LOGGER_SIZE == 0) {
                    LOGGER.info(" commit line: " + lineNum + "; queue size: " + queue.size());
                }
            }

            if (sb.length() > 0) {
                output.write(sb.toString().getBytes("utf-8"));
                output.flush();
                sb.delete(0, sb.length());
            }

            IOUtils.closeQuietly(output);
            LOGGER.info(" commit line end: " + lineNum);
            this.hiveDao.loadFileToHive(pgHiveDto.getTgtDataSourceName(), dirParent + "/" + dst.getName(), pgHiveDto.getTgtSchemaName(), pgHiveDto.getTgtTableName(), pgHiveDto.getPartitionColumnValue());
        } catch (Exception e) {
            LOGGER.error(" submit data error" ,e);
            throw e;
        } finally {
            this.isRunning.set(false);
        }
        LOGGER.info(String.format("SubmitDataToDatabase used %s second times", (System.currentTimeMillis() - beginTime) / 1000.00));
        return true;
    }

}