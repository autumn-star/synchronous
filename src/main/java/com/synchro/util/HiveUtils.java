package com.synchro.util;

import com.synchro.common.constant.HiveDivideConstant;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveUtils {
	public static String replaceBlank(String str) {
		String dest = "";
		if (str != null) {
			Pattern p = Pattern.compile(HiveDivideConstant.HIVE_DIVIDE);
			Matcher m = p.matcher(str);
			dest = m.replaceAll("");
		}
		return dest;
	}
	
	public static void main(String[] args) {
		String tmp = "liqiu\t\r\ndddd";
		System.out.println(tmp);
		System.out.println(HiveUtils.replaceBlank(tmp));
	}
}
