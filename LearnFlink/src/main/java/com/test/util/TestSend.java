package com.test.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.io.UnsupportedEncodingException;

public class TestSend {
	private static FlinkJobManager flinkJobManager = FlinkJobManagerImp.getInstance();
	private static JsonParser jsonParser = new JsonParser();
    public static void main(String[] args) {
    	String jobid = "384d9f83ffb6f6c72ae71211c98adc15";

		/**
		 * 提交job
		 */
		testMethod1();
//        testMethod2();
		/**
		 * 触发检查点
		 */
//		testMethod3();

//		testMethod4();
		/**
		 * 获取检查点状态
		 */
//		testMethod5(jobid,"");

//		testMethod6(jobid);
    }




	public static void testMethod1(){
        ReadResult readResultA = flinkJobManager.uploadJob("E:\\Asunjihua\\idea\\flink-master/LearnFlink/target/LearnFlink-jar-with-dependencies.jar","LearnFlink-jar-with-dependencies.jar");
        System.out.println(readResultA.getResponseBody());
    }

    public static void testMethod2(){
        String jobname = "7e66cf5a-556c-4b86-8395-87378f6db03c_testFlink.jar";
        String main_class = "com.test.learnWindows.TestMain1";
        try {
            ReadResult readResult = flinkJobManager.runJob(jobname,main_class,"",10);
            System.out.println(readResult.getResponseBody());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

	/**
	 * 触发检查点
	 */
	public static void testMethod3(String jobid){
		ReadResult readResult = flinkJobManager.triggerSavepoints(jobid);
		System.out.println(readResult.getResponseBody());
	}

	/**
	 * 得到检查点执行的状态
	 */
	public static void testMethod5(String jobId,String triggerId) {
		ReadResult readResult =flinkJobManager.getSavepointStatus("0f49a1dbb078e525c752485d05b57fe2","07d4ad57bc43476d8150d60b6f4f66da");
		System.out.println(readResult.getResponseBody());

	}

	/**
	 * 测试安全点并查看状态
	 */
	public static void testMethod6(String jobid) {
		ReadResult readResult = flinkJobManager.triggerSavepoints(jobid);
		if (readResult.getResponseCode() == 202){
			JsonElement jsonElement = jsonParser.parse(readResult.getResponseBody());
			String triggerId = jsonElement.getAsJsonObject().get("request-id").getAsString();
			if (triggerId != null){
				ReadResult readResult2 =flinkJobManager.getSavepointStatus(jobid,triggerId);
				System.out.println(readResult2.getResponseBody());
			}
		}
	}

	/**
	 * 得到job详情
	 */
	public static void testMethod4(){
		ReadResult readResult =flinkJobManager.getJobsDetail("d706815e316b7e34104a95421e3bb351");
	}


}
