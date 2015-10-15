package com.synchro.dal.dto;

import com.synchro.common.constant.SyncroModeEnum;

/**
 * @Info 参数基础类
 * create by liqiu on 2015-05-01
 */
public class SyncOptionsDto {

    /**
     * 模式
     */
    private SyncroModeEnum SyncMode; // 同步类型

    /**
     * 基本参数
     */
    private String srcDataSourceName; // 源数据源名称
    private String srcSchemaName; // 源shema名称
    private String srcTableName; // 源数据表名称
    private String tgtDataSourceName; // 目标据源名称
    private String tgtSchemaName; // 目标数据库名称
    private String tgtTableName; // 目标数据表名称

    /**
     * 分区
     */
    private String partitionColumnName; // 分区字段名称
    private String partitionColumnValue; // 分区字段值

    /**
     * 条件
     */
    private String where;// where条件

    /**
     * 需要同步的字段
     */
    private String columns; // 需要同步的字段

    private String excludeColumns; //需要排除的字段

    private boolean direct;

    private String splitByColumn;

    private int queueSize;


    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public SyncroModeEnum getSyncMode() {
        return SyncMode;
    }

    public void setSyncMode(SyncroModeEnum syncMode) {
        SyncMode = syncMode;
    }

    public String getWhere() {
        return where;
    }

    public String getPartitionColumnValue() {
        return partitionColumnValue;
    }

    public void setPartitionColumnValue(String partitionColumnValue) {
        this.partitionColumnValue = partitionColumnValue;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getSrcDataSourceName() {
        return srcDataSourceName;
    }

    public void setSrcDataSourceName(String srcDataSourceName) {
        this.srcDataSourceName = srcDataSourceName;
    }

    public String getSrcSchemaName() {
        return srcSchemaName;
    }

    public void setSrcSchemaName(String srcSchemaName) {
        this.srcSchemaName = srcSchemaName;
    }

    public String getSrcTableName() {
        return srcTableName;
    }

    public void setSrcTableName(String srcTableName) {
        this.srcTableName = srcTableName;
    }

    public String getPartitionColumnName() {
        return partitionColumnName;
    }

    public void setPartitionColumnName(String partitionColumnName) {
        this.partitionColumnName = partitionColumnName;
    }

    public String getTgtDataSourceName() {
        return tgtDataSourceName;
    }

    public void setTgtDataSourceName(String tgtDataSourceName) {
        this.tgtDataSourceName = tgtDataSourceName;
    }

    public String getTgtSchemaName() {
        return tgtSchemaName;
    }

    public void setTgtSchemaName(String tgtSchemaName) {
        this.tgtSchemaName = tgtSchemaName;
    }

    public String getTgtTableName() {
        return tgtTableName;
    }

    public void setTgtTableName(String tgtTableName) {
        this.tgtTableName = tgtTableName;
    }

    public boolean isDirect() {
        return direct;
    }

    public void setDirect(boolean direct) {
        this.direct = direct;
    }

    public String getSplitByColumn() {
        return splitByColumn;
    }

    public void setSplitByColumn(String splitByColumn) {
        this.splitByColumn = splitByColumn;
    }

    public String getExcludeColumns() {
        return excludeColumns;
    }

    public void setExcludeColumns(String excludeColumns) {
        this.excludeColumns = excludeColumns;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    @Override
    public String toString() {
        return "FromPgToHiveDto{" + "srcDataSourceName='" + srcDataSourceName + '\'' + ", srcSchemaName='" + srcSchemaName + '\'' + ", srcTableName='" + srcTableName
                + '\'' + ", partitionColumnName='" + partitionColumnName + '\'' + ", tgtDataSourceName='" + tgtDataSourceName + '\'' + ", tgtSchemaName='"
                + tgtSchemaName + '\'' + ", tgtTableName='" + tgtTableName + '\'' + '}';
    }

}
