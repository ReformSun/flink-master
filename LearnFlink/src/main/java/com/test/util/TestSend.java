package com.test.util;

import java.io.UnsupportedEncodingException;

public class TestSend {
    public static void main(String[] args) {
        testMethod1();
//        testMethod2();

    }


    public static void testMethod1(){
        FlinkJobManager flinkJobManager = FlinkJobManagerImp.getInstance();
        ReadResult readResultA = flinkJobManager.uploadJob("./target/testMain1.jar","testMain1.jar");
        System.out.println(readResultA.getResponseBody());
    }

    public static void testMethod2(){
        FlinkJobManager flinkJobManager = FlinkJobManagerImp.getInstance();
        String jobname = "45fe5141-ee41-4b67-b38e-56351b107505_testMain1.jar";
        String main_class = "com.test.learnWindows.TestMain1";
        try {
            ReadResult readResult = flinkJobManager.runJob(jobname,main_class,"",10);
            System.out.println(readResult.getResponseBody());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static void testMethod3(){

    }


}
