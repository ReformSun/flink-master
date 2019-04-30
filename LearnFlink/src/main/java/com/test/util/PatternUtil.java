package com.test.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternUtil {
    // 年月日 时分秒
    public static String year = "[0-9]{1}[0-9]{3}";
    public static String month = "([1-9]{1}|0{1}[0-9]{1}|1{1}[0-2]{1})";// 1~9 01~09 10~12
    public static String day = "([1-9]{1}|0{1}[0-9]{1}|[1-2]{1}[0-9]{1}|3{1}[0-1]{1})";// 1~9 01~09 10~29 30~31
    public static String hour = "([0-9]{1}|0{1}[0-9]{1}|1{1}[0-9]{1}|2{1}[0-4]{1})";// 0~9 10~19 20~24 00~09
    public static String minute = "([1-9]{1}|0{1}[0-9]{1}|[1-5]{1}[0-9]{1}|6{1}0{1})";// 1~9 10~59 60 01~09
    public static String second =  "([1-9]{1}|0{1}[0-9]{1}|[1-5]{1}[0-9]{1}|6{1}0{1})";// 1~9 10~59 60 01~09
    public static String millisecond = "([0-9]{1}|[0-9]{1}[0-9]{1}|[0-9]{1}[0-9]{1}[0-9]{1})";
    // 非数字字符匹配
    public static Pattern intgerPattern = Pattern.compile("^^[+-]{0,1}[1-9]{1}[0-9]*[0-9]{1}$");
    public static Pattern timestampPattern = Pattern.compile("^[1-2]{1}[0-9]{0,11}[0-9]{1}$");
    public static  Pattern datePattern = Pattern.compile("^" + year + ".{1}" + month + ".{1}" + day + ".{1}" + hour + ".{1}" + minute + ".{1}(" + second + "$|" + second + ".{1}" + millisecond + "$)");
    // 特殊字符匹配
    public static String specialCpatternRegEx = "[ _`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？\\\\]|\n|\r|\t";
    public static Pattern floatPattern = Pattern.compile("^[+-]{0,1}[1-9]{1}[0-9]*\\.{1}[0-9]*[0-9]{1}$");


    public static Object getValue(String data){
        Matcher matcher = floatPattern.matcher(data);
        if (matcher.find()){
            Double doubleValue = Double.valueOf(data);
            if (doubleValue < Float.MAX_VALUE){
                return Float.valueOf(data);
            }else {
                return doubleValue;
            }
        }else{
            Matcher matcher1 = intgerPattern.matcher(data);
            if (matcher1.find()){
                Long longValue = Long.valueOf(data);
                if (longValue < Integer.MAX_VALUE){
                    return Integer.valueOf(data);
                }else {
                    return longValue;
                }
            }else {
                return data;
            }
        }
    }

    public static boolean isFloat(String data){
        Matcher matcher = floatPattern.matcher(data);
        if (matcher.find()){
            Double doubleValue = Double.valueOf(data);
            if (doubleValue < Float.MAX_VALUE){
                return true;
            }
        }
        return false;
    }

    public static boolean isDouble(String data){
        Matcher matcher = floatPattern.matcher(data);
        if (matcher.find()){
            Double doubleValue = Double.valueOf(data);
            if (doubleValue > Float.MAX_VALUE){
                return true;
            }
        }
        return false;
    }

    public static boolean isInteger(String data){
        Matcher matcher = intgerPattern.matcher(data);
        if (matcher.find()){
            Long longValue = Long.valueOf(data);
            if (longValue < Integer.MAX_VALUE){
                return true;
            }
        }
        return false;
    }

    public static boolean isNotString(String data){
        if (!isDate(data)&&!isTimestamp(data)&&!isLong(data)&&!isDouble(data)&&!isFloat(data)&&!isInteger(data)){
            return false;
        }else {
            return true;
        }
    }

    public static boolean isLong(String data){
        Matcher matcher = intgerPattern.matcher(data);
        if (matcher.find()){
            Long longValue = Long.valueOf(data);
            if (longValue > Integer.MAX_VALUE){
                return true;
            }
        }
        return false;
    }


    public static boolean isTimestamp(String data){
        Matcher matcher = timestampPattern.matcher(data);
        if (matcher.find()){
            return true;
        }else {
            return false;
        }
    }

    public static boolean isDate(String data){
        Matcher matcher = datePattern.matcher(data);
        if (matcher.find()){
            return true;
        }else {
            return false;
        }
    }

    public static void main(String[] args) {
//        String date = "2019-01-14 14:05:49:992";
//        System.out.println(isDate(date));
//        date = "2019-02-13 14:06:00,101";
//        System.out.println(isDate(date));
//        date = "2019-02-14 09:00:36,617";
//        System.out.println(isDate(date));
//        date = "2019-02-14 00:00:36,617";
//        System.out.println(isDate(date));
//        date = "2019-02-00 00:00:36,617";
//        System.out.println(isDate(date));
//        date = "2019-01-14 14:05:49";
//        System.out.println(isDate(date));
//        date = "";

        System.out.println(isInteger("1"));
        Integer.decode("111111111111111111");
    }


}
