package com.test.util;

import java.io.UnsupportedEncodingException;

public class TestSend {
    public static void main(String[] args) {
        testMethod1();
//        testMethod2();

    }


    public static void testMethod1(){
        FlinkJobManager flinkJobManager = FlinkJobManagerImp.getInstance();
        ReadResult readResultA = flinkJobManager.uploadJob("/Users/apple/Documents/AgentJava/flink-master/LearnFlink/target/LearnFlink-jar-with-dependencies.jar","LearnFlink-jar-with-dependencies.jar");
        System.out.println(readResultA.getResponseBody());
    }

    public static void testMethod2(){
        FlinkJobManager flinkJobManager = FlinkJobManagerImp.getInstance();
        String jobname = "7e66cf5a-556c-4b86-8395-87378f6db03c_testFlink.jar";
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
