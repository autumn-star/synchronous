package com.synchro.service.copy;

import com.synchro.common.constant.HiveDivideConstant;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.service.DataSourceService;
import com.synchro.dal.metadata.DataSourceMetaData;
import com.synchro.dal.metadata.RowData;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.util.SpringContextUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Info copyService 执行copy
 * created by liqiu
 *
 */
public class CopyService {

    private final static Logger LOGGER = LoggerFactory.getLogger(CopyService.class);

    private LinkedBlockingQueue<RowData> queue; // 队列
    private SyncOptionsDto syncOptions; // 参数对象
    private String querySql; //copy sql
    private List<ColumnMetaData> columnMetaDatas; //字段元信息
    private DataSourceService dataSourceService; // 数据元信息

    public CopyService(LinkedBlockingQueue<RowData> queue, SyncOptionsDto syncOptions, String querySql,List<ColumnMetaData> columnMetaDatas) {
        this.queue = queue;
        this.syncOptions = syncOptions;
        this.querySql = querySql;
        this.dataSourceService = SpringContextUtils.getBean(DataSourceService.class);
        this.columnMetaDatas = columnMetaDatas;
    }

    /**
     * 将数据放入缓存队列
     */
    public void putCopyData() {
        DataSourceMetaData dataSource = dataSourceService.getDataSource(syncOptions.getSrcDataSourceName());
        String copyCommand = this.getCopyCommand(dataSource, querySql); //获取copy命令
        ShellExecuter.execute(copyCommand, queue,columnMetaDatas);
    }

    /**
     * 执行copy的shell命令
     * @param dataSource
     * @param sql
     * @return
     */
    public String getCopyCommand(DataSourceMetaData dataSource, String sql){
        String host = dataSource.getIp();
        String user = dataSource.getUserName();
        String dataBaseName = dataSource.getDatabaseName();
        //String psqlPath = "/Library/PostgreSQL/9.3/bin/psql";
        String psqlPath = "/opt/pg93/bin/psql";
        String execCopy = psqlPath + " -h " + host + " -U " + user + " " + dataBaseName +" -c \"COPY (" + sql + ") TO STDOUT WITH DELIMITER E'"+ HiveDivideConstant.COPY_COLUMN_DIVIDE+"' CSV NULL AS E'NULL'\" "; // 执行copy命令
        LOGGER.info(execCopy);
        return execCopy;
    }

}