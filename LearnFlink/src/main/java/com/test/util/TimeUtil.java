package com.test.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * UTC 时间格式 yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
 */
public class TimeUtil {
	// 获取本地时区
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();
    private static final String defaultDateFormat = "yyyy-MM-dd HH:mm:ss:SSS";
    private static final String UTCFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static long toLong(Timestamp v) {
        return toLong(v, LOCAL_TZ);
    }
    public static long toLong(Date v, TimeZone timeZone) {
        long time = v.getTime();
        return time + (long)timeZone.getOffset(time);
    }

	public static long toLong(long time, TimeZone timeZone) {
		return time + (long)timeZone.getOffset(time);
	}

	public static long toLong(long time) {
		return toLong(time,LOCAL_TZ);
	}

	public static long toUTC(long time){
    	return time - LOCAL_TZ.getOffset(time);
	}

	public static String toDate(long time){
		return toDate(time,null);
	}

	public static String toDate(long time,String dateFormat){
		if (dateFormat == null){
			dateFormat = defaultDateFormat;
		}
		Date date = new Date(time);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.format(date);
	}

	public static long toLong(String time){
		return toLong(time,null);
	}
	public static long toLong(String time,String dateFormat){
		if (dateFormat == null){
			dateFormat = defaultDateFormat;
		}
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		try {
			Date date1 = simpleDateFormat.parse(time);
			return date1.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0L;
	}

	public static void main(String[] args) {
		System.out.println(toUTC(1534500840000L));
		System.out.println(toDate(1534500840000L,UTCFormat));
		System.out.println(toDate(1534500840000L,"yyyy-MM-dd HH:mm:ss"));
//		System.out.println(toLong("1534500840","yyyy-MM-dd HH:mm:ss"));
	}
}
