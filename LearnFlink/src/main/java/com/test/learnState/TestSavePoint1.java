package com.test.learnState;

import org.apache.flink.core.fs.local.LocalDataInputStream;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1Serializer;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;

import java.io.*;

public class TestSavePoint1 {
	private static String path = "/Users/apple/Desktop/state/savepointData/savepoint-9a2537-45f630f3a374/";
	public static void main(String[] args) {
//		testMethod1();
		testMethod2();
	}

	/**
	 *
	 * 不能使用
	 */
	public static void testMethod1(){
		SavepointV1Serializer serializer = SavepointV1Serializer.INSTANCE;

		try {
			FileInputStream fileInputStream = new FileInputStream(path+"4db77ce8-0175-403b-87a8-bdb176a43475");
			DataInputStream dataInputStream = new DataInputStream(fileInputStream);
			SavepointV2 savepointV2 = serializer.deserialize(dataInputStream,serializer.getClass().getClassLoader());
			System.out.println(savepointV2);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 可以
	 */
	public static void testMethod2(){
//		Checkpoints
		String path = "/Users/apple/Desktop/state/savepointData/savepoint-6c7bd9-73bbcfafd18c/_metadata";

		try {
			LocalDataInputStream localDataInputStream = new LocalDataInputStream(new File(path));
//			FileInputStream fileInputStream = new FileInputStream(path);
			DataInputStream dataInputStream = new DataInputStream(localDataInputStream);
			Savepoint savepointV2 = Checkpoints.loadCheckpointMetadata(dataInputStream,localDataInputStream.getClass().getClassLoader());
			System.out.println(savepointV2);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
