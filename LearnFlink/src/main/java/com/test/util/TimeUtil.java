package com.test.util;

import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

public class TimeUtil {
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();
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

	public static void main(String[] args) {
		System.out.println(toUTC(1534500840000L));
	}
}
