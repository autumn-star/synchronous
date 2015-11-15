package com.synchro.common.constant;

/**
 * 定义分隔符的类
 * 
 * @author qiu.li
 * @Modified time 2015-08-04
 * 
 */
public interface HiveDivideConstant {

	final static String COLUMN_DIVIDE = "\\001"; // 字段分隔符

	final static String LINE_DIVIDE = "\\n"; // 换行分隔符

	final static String HIVE_DIVIDE = "\t|\r|\n"; // 同步数据到hive时候，需要替换的字符

	final static String ENTER = "\n";

	final static Character COPY_COLUMN_DIVIDE = '';
}
