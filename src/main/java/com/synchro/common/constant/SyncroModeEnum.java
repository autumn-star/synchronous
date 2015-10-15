package com.synchro.common.constant;

/**
 * 同步方式
 * @CreateTime: 2015-08-08
 * @author liqiu
 *
 */
public enum SyncroModeEnum {

	ALL(1), // 全量
	TIME(2), // 时间
	ID(3), // id分区
	COPY(4) // 拷贝方式
	;
	
	// 定义私有变量
	private int nCode;

	// 构造函数，枚举类型只能为私有
	private SyncroModeEnum(int _nCode) {
		this.nCode = _nCode;
	}

	@Override
	public String toString() {
		return String.valueOf(this.nCode);
	}
}