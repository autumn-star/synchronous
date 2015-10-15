package com.synchro.dal.metadata;

/**
 * @description 行信息统计
 * @author qiu.li
 * @since 2015-03-25
 * 
 */
public class RowData {
	private Object[] columnObjects; // 字段数组
	private String value; // 所有字段的value
	private static String divide = "','";

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = this.toString();
	}

	public void setColumnObjects(Object[] columnObjects) {
		this.columnObjects = columnObjects;
	}

	/**
	 * 构造方法，直接生成value
	 * @param columnObjects
	 */
	public RowData(Object... columnObjects) {
		this.columnObjects = columnObjects;
		this.value = this.toString();
	}

	public Object[] getColumnObjects() {
		return this.columnObjects;
	}

	@Override
	public String toString() {
		StringBuffer strBuff = new StringBuffer();
		for (Object obj : this.columnObjects) {
			strBuff.append(obj);
			strBuff.append(RowData.divide);
		}
		return strBuff.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RowData other = (RowData) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

}
