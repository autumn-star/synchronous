package com.synchro.service;

import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.TableMetaData;

import java.util.List;

public class SqlService {

	public static String createInsertPreparedSql(TableMetaData tableMetaData) {
		List<ColumnMetaData> columnMetaDatas = tableMetaData.getColumnMetaDatas();

		StringBuffer columnStrBuff = new StringBuffer();
		StringBuffer valueStrBuff = new StringBuffer();

		for (int i = 0; i < columnMetaDatas.size(); i++) {
			if (i == 0) {
				columnStrBuff.append(columnMetaDatas.get(i).getName());
				valueStrBuff.append("?");
			} else {
				columnStrBuff.append(", ");
				columnStrBuff.append(columnMetaDatas.get(i).getName());
				valueStrBuff.append(", ");
				valueStrBuff.append("?");
			}
		}
		String insertSql =String.format("insert into %s.%s(%s) values (%s)", tableMetaData.getSchema(), tableMetaData.getTableName(), columnStrBuff.toString(), valueStrBuff.toString());;
		return insertSql;
	}

	public static String createDeleteSql(TableMetaData tableMetaData){

		StringBuilder deleteSql = new StringBuilder();
		deleteSql.append(" delete from ").append(tableMetaData.getSchema()).append(".").
				append(tableMetaData.getTableName()).append(" where id = ?");
		return deleteSql.toString();
	}

}