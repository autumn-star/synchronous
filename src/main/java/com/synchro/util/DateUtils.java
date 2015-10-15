package com.synchro.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Created with IntelliJ IDEA. User: xingxing.duan Date: 14-7-8 Time: 下午3:24
 */
public class DateUtils {

	private static final Logger logger = LoggerFactory.getLogger(DateUtils.class);
	public static String YEAR = "year";
	public static String MONTH = "month";
	public static String DAY = "day";
	public static String HOUR = "hour";
	public static String MINUTE = "minute";
	public static String SECOND = "second";

	public static int INTERVAL_JUST = 0;
	public static int INTERVAL_10M = 1;
	public static int INTERVAL_30M = 2;
	public static int INTERVAL_1H = 3;
	public static int INTERVAL_2H = 4;
	public static int INTERVAL_3H = 5;
	public static int INTERVAL_5H = 6;
	public static int INTERVAL_1D = 7;

	public static String DATE_FORMAT_yyyyMMdd = "yyyy-MM-dd";
	public static String DATE_FORMAT_yyyyMMdd_hhmmss = "yyyy-MM-dd HH:mm:ss";
	public static String DATE_FORMAT_yyyyMMdd_HHmmssSSSZ = "yyyy-MM-dd HH:mm:ss.SSSZ";

	public static Date getCurrentDate() {
		Date date = new Date(System.currentTimeMillis());
		return date;
	}

	public static String dateToString(Date date, String formatStr) {
		SimpleDateFormat formatDate = new SimpleDateFormat(formatStr);
		String str = formatDate.format(date);
		return str;
	}

	public static Date stringToDate(String dateStr, String formatStr) {
		DateFormat sdf = new SimpleDateFormat(formatStr, Locale.ENGLISH);
		Date date = null;
		try {
			date = sdf.parse(dateStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return date;
	}

	public static Date stringToDate(String dateStr) {
		if (dateStr==null || dateStr.length()<10)	return null;
		if (dateStr.length()>10) dateStr = dateStr.substring(0, 10);
		
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
		try {
			date = sdf.parse(dateStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return date;
	}

	public static java.sql.Date stringToSqlDate(String dateStr) {
		if (dateStr==null || dateStr.length()<10)	return null;
		if (dateStr.length()>10) dateStr = dateStr.substring(0, 10);
		return java.sql.Date.valueOf(dateStr); 
	}

	public static String dateToString(Date date) {
		SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");
		String str = formatDate.format(date);
		return str;
	}

	public static int getWeekInYear(String date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setFirstDayOfWeek(Calendar.MONDAY);
		calendar.set(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek());
		calendar.setTime(DateUtils.stringToDate(date));
		return calendar.get(Calendar.WEEK_OF_YEAR);
	}

	/**
	 * 07/Aug/2012:16:00:02----> 2012-08-07 16:00:02
	 *
	 * @param date
	 */
	public static String dateConvert(String date) {
		SimpleDateFormat in = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
		SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date d = in.parse(date);
			return out.format(d);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * date之后num天，num可以为负数，负数表示之前
	 *
	 * @param date
	 * @param num
	 * @return
	 */
	public static Date addDay(Date date, int num) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, num);
		return cal.getTime();
	}

	/**
	 * date之后num天，num可以为负数，负数表示之前
	 *
	 * @param date
	 * @param num
	 * @return
	 */
	public static String addDayStr(Date date, int num) {
		Date dt = addDay(date, num);
		return dateToString(dt);
	}

	public static Map<String, Object> getIntervalOfDates(long second) {
		Map<String, Object> map = new HashMap<String, Object>();
		long minute = second / 60;
		long hour = minute / 60;
		long day = hour / 24;
		if (day > 0) {
			map.put("type", "day");
			map.put("interval", day);
		} else if (hour > 0) {
			map.put("type", "hour");
			map.put("interval", hour);
		} else {
			map.put("type", "minute");
			map.put("interval", minute);
		}
		return map;
	}

	public static long getIntervalOfTwoDates(Date beginDate, Date endDate) {
		if (beginDate == null || endDate == null)
			return 0;
		long endTime = endDate.getTime();
		long beginTime = beginDate.getTime();
		long intervalMilliSecond = endTime > beginTime ? endTime - beginTime : 0;
		long intervalSecond = intervalMilliSecond / 1000;
		return intervalSecond;
	}

	public static int getIntervalDaysOfTwoDates(Date beginDate, Date endDate) {
		if (beginDate == null || endDate == null)
			return 0;
		long endTime = endDate.getTime();
		long beginTime = beginDate.getTime();
		long intervalMilliSecond = endTime > beginTime ? endTime - beginTime : 0;
		return new Long(intervalMilliSecond / 86400000).intValue();
	}

	public static Date getDateBeforeHours(Date comparedDate, int cursor, String unit) {
		if (unit.equalsIgnoreCase("hour")) {
			long interval = new Long(cursor);
			long millisecond = comparedDate.getTime() - interval * 3600 * 1000;
			return new Date(millisecond);
		} else if (unit.equalsIgnoreCase("day")) {
			long interval = new Long(cursor);
			long millisecond = comparedDate.getTime() - interval * 3600 * 1000 * 24;
			return new Date(millisecond);
		} else {
			long millisecond = comparedDate.getTime() - cursor * 1000;
			return new Date(millisecond);
		}
	}

	public static String deSerialize(Date date, String pattern) {
		if (date == null)
			return "";
		String defaultPattern = "yyyy-MM-dd HH:mm";
		if (pattern == null)
			pattern = defaultPattern;
		SimpleDateFormat formatter = new SimpleDateFormat(pattern);
		return formatter.format(date);
	}

	public static Date serialize(String dateStr, String pattern) throws ParseException {
		String defaultPattern = "yyyy-MM-dd HH:mm";
		if (pattern == null)
			pattern = defaultPattern;
		SimpleDateFormat formatter = new SimpleDateFormat(pattern);
		return formatter.parse(dateStr);
	}

	/**
	 * 获得date日期该天的开始时间 例如：2011-04-27 00:00:00是2011-04-27的开始时间
	 *
	 * @param date
	 * @return
	 */
	public static Date getBeginTimeForDate(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		return c.getTime();
	}

	/**
	 * 获得date日期该天的结束时间 例如：2011-04-27 23:59:59是2011-04-27的结束时间
	 *
	 * @param date
	 * @return
	 */
	public static Date getEndTimeForDate(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.HOUR_OF_DAY, 23);
		c.set(Calendar.MINUTE, 59);
		c.set(Calendar.SECOND, 59);
		return c.getTime();
	}

	public static Date getBefore(Date comparedDate, int cursor, String unit) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(comparedDate);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int date = calendar.get(Calendar.DATE);
		int hour = calendar.get(Calendar.HOUR);
		int minute = calendar.get(Calendar.MINUTE);
		int second = calendar.get(Calendar.SECOND);
		if (unit.equalsIgnoreCase(SECOND)) {
			second = second - cursor;
		} else if (unit.equalsIgnoreCase(MINUTE)) {
			minute = minute - cursor;
		} else if (unit.equalsIgnoreCase(HOUR)) {
			hour = hour - cursor;
		} else if (unit.equalsIgnoreCase(DAY)) {
			date = date - cursor;
		} else if (unit.equalsIgnoreCase(MONTH)) {
			month = month - cursor;
		} else if (unit.equalsIgnoreCase(YEAR)) {
			year = year - cursor;
		}
		// calendar.set(year, month, date, hour, minute, second);
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, month);
		calendar.set(Calendar.DAY_OF_MONTH, date);
		calendar.set(Calendar.HOUR_OF_DAY, hour);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
		return calendar.getTime();
	}

	public static Date getAfter(Date comparedDate, int cursor, String unit) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(comparedDate);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int date = calendar.get(Calendar.DATE);
		int hour = calendar.get(Calendar.HOUR);
		int minute = calendar.get(Calendar.MINUTE);
		int second = calendar.get(Calendar.SECOND);
		if (unit.equalsIgnoreCase(SECOND)) {
			second += cursor;
		} else if (unit.equalsIgnoreCase(MINUTE)) {
			minute += cursor;
		} else if (unit.equalsIgnoreCase(HOUR)) {
			hour += cursor;
		} else if (unit.equalsIgnoreCase(DAY)) {
			date += cursor;
		} else if (unit.equalsIgnoreCase(MONTH)) {
			month += cursor;
		} else if (unit.equalsIgnoreCase(YEAR)) {
			year += cursor;
		}
		// calendar.set(year, month, date, hour, minute, second);
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, month);
		calendar.set(Calendar.DAY_OF_MONTH, date);
		calendar.set(Calendar.HOUR_OF_DAY, hour);
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.SECOND, second);
		return calendar.getTime();
	}

	/**
	 * 获取当前时间的前一天日期
	 *
	 * @return
	 */
	public static String getPreviousDate() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		return df.format(cal.getTime());
	}

	/**
	 * 获取当前时间的前一天日期的开始时间
	 *
	 * @return
	 */
	public static String getPreviousDateBeginTime() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		return df.format(cal.getTime());
	}

	/**
	 * 获取当前时间的前一天日期的结束时间
	 *
	 * @return
	 */
	public static String getPreviousDateEndTime() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd 23:59:59");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		return df.format(cal.getTime());
	}

	/**
	 * 把20140910类型的日期格式转换为2014-09-10
	 *
	 * @return
	 */
	public static String parserDateStr(String dateStr) {
		return String.format("%s-%s-%s", dateStr.substring(0, 4), dateStr.substring(4, 6), dateStr.substring(6, 8));
	}

	/**
	 * 获取某个日期的开始时间：2014-09-10 得到 2014-09-10 00:00:00
	 *
	 * @param dateStr
	 * @return
	 */
	public static String getDateBeginTime(String dateStr) {
		return dateStr + " 00:00:00";
	}

	/**
	 * 获取某个日期的结束时间：2014-09-10 得到 2014-09-10 23:59:59
	 *
	 * @param dateStr
	 * @return
	 */
	public static String getDateEndTime(String dateStr) {
		return dateStr + " 23:59:59";
	}

	/**
	 * 获取某个日期字符串的前一天 2014-09-10得到2014-09-09
	 *
	 * @param dateStr
	 * @return
	 */
	public static String getPreviousDateStr(String dateStr) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date date = df.parse(dateStr);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			cal.add(Calendar.DAY_OF_MONTH, -1);
			return df.format(cal.getTime());
		} catch (ParseException e) {
			return "";
		}
	}

	/**
	 * 将YYYY-MM-DD类型的日期字符串，转换成YYYY_MM_DD类型的日期字符串
	 *
	 * @param dateStr
	 * @return
	 */
	public static String getTableNameCurrDateStr(String dateStr) {
		return dateStr.replaceAll("-", "_");
	}

	/**
	 * 将YYYY-MM-DD类型的日期字符串，转换成YYYYMM类型的月份字符串
	 *
	 * @param dateStr
	 * @return
	 */
	public static String getTableNameCurrYearMonthStr(String dateStr) {
		String tmpStr = dateStr.replaceAll("-", "");
		return tmpStr.substring(0, 6);
	}

	public static String getDateFormatFold() {
		Date date = new Date(System.currentTimeMillis());
		return dateToString(date, "yyyy/MM/dd/");
	}


	/**
	 * Get the Dates between Start Date and End Date.
	 *
	 * @param p_start
	 *            Start Date
	 * @param p_end
	 *            End Date
	 * @return Dates List
	 */
	public static List<Date> getDates(Date p_start, Date p_end) {
		List<Date> result = new ArrayList<Date>();
		result.add(p_start);
		Calendar temp = Calendar.getInstance();
		temp.setTime(p_start);
		Calendar end = Calendar.getInstance();
		end.setTime(p_end);
		temp.add(Calendar.DAY_OF_YEAR, 1);
		while (temp.before(end)) {
			result.add(temp.getTime());
			temp.add(Calendar.DAY_OF_YEAR, 1);
		}
		result.add(p_end);
		return result;
	}

	public static Timestamp getTimestamp(String str) throws Exception {
		String formatString = DateUtils.DATE_FORMAT_yyyyMMdd;
		if (str.length()>10){
			formatString = DateUtils.DATE_FORMAT_yyyyMMdd_hhmmss;
		}
		DateFormat format = new SimpleDateFormat(formatString);
		format.setLenient(false);
		Timestamp ts = null;
		try {
			ts = new Timestamp(format.parse(str).getTime());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info("(str:"+str+")getTimestamp error: " + e.toString());
			throw e;
		}
		return ts;
	}
	
	
	

	public static void main(String[] args) {
		System.out.println(DateUtils.stringToSqlDate("2014-09-01 10"));
	}
	
}
