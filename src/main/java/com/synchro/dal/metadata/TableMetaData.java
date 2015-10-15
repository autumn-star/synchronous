package com.synchro.dal.metadata;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author qiu.li
 * @description 基本数据表信息
 * @since 2015-03-25
 */
public class TableMetaData {
    private String datasource;
    private String schema; // shema，在hive中是数据库
    private String tableName; // 数据表名称
    private String columns;
    private String where;
    private String partitionColumn; // 分区字段
    private List<ColumnMetaData> columnMetaDatas; // 字段数组
    private final static Joiner joiner = Joiner.on(",");


    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public TableMetaData() {
        this.columnMetaDatas = new ArrayList<ColumnMetaData>();
    }

    public TableMetaData(String schema, String table) {
        this.schema = schema;
        this.tableName = table;
        this.columnMetaDatas = new ArrayList<ColumnMetaData>();
    }

    public TableMetaData(String datasource, String schema, String columns,String tableName, String where) {
        this.datasource = datasource;
        this.schema = schema;
        this.columns = columns;
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

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public List<ColumnMetaData> getColumnMetaDatas() {
        return columnMetaDatas;
    }

    public void setColumnMetaDatas(List<ColumnMetaData> columnMetaDatas) {
        this.columnMetaDatas = columnMetaDatas;
    }

    /**
     * 获取按照逗号分开的字段字符串
     *
     * @return
     */
    public String getColumnsStr() {
        if(StringUtils.isNotBlank(columns)){
            return  this.columns;
        }else{
            List<String> columnArray = Lists.transform(this.getColumnMetaDatas(), new Function<ColumnMetaData, String>() {
                @Override
                public String apply(ColumnMetaData input) {
                    return input.getName();
                }
            });
            return joiner.join(columnArray);
        }

    }

    @Override
    public String toString() {
        return this.getColumns();

    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            // 不为空
            return false;
        }
        if (this.getClass() == obj.getClass()) {
            // 类型一致
            TableMetaData t = (TableMetaData) obj;
            if (this.getColumnMetaDatas().size() == t.getColumnMetaDatas().size()) {
                // 大小相等
                for (int i = 0; i < this.getColumnMetaDatas().size(); i++) {
                    // 逐一比较
                    if (!this.getColumnMetaDatas().get(i).equals(t.getColumnMetaDatas().get(i))) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

}
