package com.synchro.dal.metadata;

import java.util.LinkedList;

public class HiveTableMetaData {

	private String dataName; // 数据库名称
	private String tableName; // 数据表名称
	private LinkedList<ColumnMetaData> columns; // 所有字段名称

	public String getDataName() {
		return dataName;
	}

	public void setDataName(String dataName) {
		this.dataName = dataName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public LinkedList<ColumnMetaData> getColumns() {
		return columns;
	}

	public void setColumns(LinkedList<ColumnMetaData> columns) {
		this.columns = columns;
	}

}