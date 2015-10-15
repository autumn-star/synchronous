package com.synchro.dal.condition;

import com.synchro.dal.metadata.TableMetaData;

/**
 * Created by xingxing.duan on 2015/8/24.
 */
public class TableMetaDataCondition {

    private String datasource;
    private String schema; // shema，在hive中是数据库
    private String tableName; // 数据表名称
    private String columns;
    private String excludeColumns;
    private String where;
    private String partitionColumn; // 分区字段


    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }


    public TableMetaDataCondition(String datasource, String schema, String columns, String tableName, String where) {
        this.datasource = datasource;
        this.schema = schema;
        this.columns = columns;
        this.tableName = tableName;
        this.where = where;
    }

    public TableMetaDataCondition(String datasource, String schema, String columns, String excludeColumns, String tableName, String where) {
        this.datasource = datasource;
        this.schema = schema;
        this.columns = columns;
        this.excludeColumns = excludeColumns;
        this.tableName = tableName;
        this.where = where;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public String getExcludeColumns() {
        return excludeColumns;
    }

    public void setExcludeColumns(String excludeColumns) {
        this.excludeColumns = excludeColumns;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    @Override
    public String toString() {
        return this.getColumns();

    }

    public TableMetaData convert() {

        TableMetaData tableMetaData = new TableMetaData();
        tableMetaData.setDatasource(this.datasource);
        tableMetaData.setSchema(this.schema);
        tableMetaData.setColumns(this.columns);
        tableMetaData.setTableName(this.tableName);
        tableMetaData.setWhere(this.where);
        return tableMetaData;
    }
}
