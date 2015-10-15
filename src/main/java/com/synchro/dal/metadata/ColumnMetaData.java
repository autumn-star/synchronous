package com.synchro.dal.metadata;

import java.sql.SQLException;
import java.sql.Types;

import org.springframework.jdbc.support.rowset.SqlRowSet;


/**
 * @description 元字段
 * @author qiu.li
 * @since 2015-03-25
 * 
 */
public class ColumnMetaData implements Comparable<ColumnMetaData> {

	private String name; // 字段名称
	private int type; // 字段类型
	private String typeName; // 字段类型名称
	private Object value; // 值

	public ColumnMetaData() {
	}

	public ColumnMetaData(String name) {
		this.name = name;
	}

	public ColumnMetaData(String columnName, int valueType) {
		this.name = columnName;
		this.type = valueType;
	}

	public ColumnMetaData(String columnName, int valueType, String typeName) {
		this.name = columnName;
		this.type = valueType;
		this.typeName = typeName;
	}

	@Override
	public boolean equals(Object o) {
		// 和自己比较
		if (this == o)
			return true;

		// 类型不相符
		if (o == null || getClass() != o.getClass())
			return false;

		ColumnMetaData that = (ColumnMetaData) o;

		// name是否相等
		if (name != null ? !name.equals(that.name) : that.name != null)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		return name != null ? name.hashCode() : 0;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public int compareTo(ColumnMetaData o) {
		return this.getName().compareTo(o.getName());
	}

	@Override
	public String toString() {
		return name;

	}

	/**
	 * 根据类型获取游标内的值
	 * @param resultSet
	 * @param columnMetaData
	 * @return
	 * @throws SQLException
	 */
	public static Object getColumnValue(SqlRowSet resultSet, ColumnMetaData columnMetaData) throws SQLException {
		Object object = null;

		switch (columnMetaData.getType()) {

		case Types.BIGINT:
			object = resultSet.getLong(columnMetaData.getName());
			break;
		case Types.BIT:
			object = resultSet.getBoolean(columnMetaData.getName());
			break;
		case Types.BOOLEAN:
			object = resultSet.getBoolean(columnMetaData.getName());
			break;
		case Types.CHAR:
			object = resultSet.getString(columnMetaData.getName());
			break;
		case Types.CLOB:
			object = resultSet.getString(columnMetaData.getName());
			break;
		case Types.DATALINK:
			break;
		case Types.TIMESTAMP:
			object = resultSet.getTimestamp(columnMetaData.getName());
			break;
		case Types.DATE:
			object = resultSet.getDate(columnMetaData.getName());
			break;
		case Types.DECIMAL:
			object = resultSet.getBigDecimal(columnMetaData.getName());
			break;
		case Types.DISTINCT:
			break;
		case Types.DOUBLE:
			object = resultSet.getDouble(columnMetaData.getName());
			break;
		case Types.FLOAT:
			object = resultSet.getFloat(columnMetaData.getName());
			break;
		case Types.INTEGER:
			object = resultSet.getInt(columnMetaData.getName());
			break;
		case Types.JAVA_OBJECT:
			object = resultSet.getObject(columnMetaData.getName());
			break;
		case Types.LONGNVARCHAR:
			break;
		case Types.LONGVARBINARY:
			break;
		case Types.NCHAR:
			break;
		case Types.NCLOB:
			break;
		case Types.NULL:
			break;
		case Types.NUMERIC:
			object = resultSet.getBigDecimal(columnMetaData.getName());
			break;
		case Types.NVARCHAR:
			break;
		case Types.OTHER:
			object = resultSet.getObject(columnMetaData.getName());
			break;
		case Types.REAL:
			object = resultSet.getFloat(columnMetaData.getName());
			break;
		case Types.REF:
			break;
		case Types.ROWID:
			break;
		case Types.SMALLINT:
			object = resultSet.getInt(columnMetaData.getName());
			break;
		case Types.SQLXML:
			break;
		case Types.STRUCT:
			break;
		case Types.TIME:
			object = resultSet.getTime(columnMetaData.getName());
			break;
		case Types.TINYINT:
			object = resultSet.getInt(columnMetaData.getName());
			break;
		case Types.VARBINARY:
			break;
		case Types.VARCHAR:
			object = resultSet.getString(columnMetaData.getName());
			break;
		default:
			object = resultSet.getString(columnMetaData.getName());
			break;

		}

		return object;
	}
}