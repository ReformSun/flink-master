package com.test.asyncFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CustomAsyncFunctonTuple3 extends RichAsyncFunction<Tuple3<String,Integer,Long>, Tuple3<String,String,Long>>{

//	private static Statement stmt;
	private String sql = "select mapvalue from \"public\".\"flink_map\" where mapname =";
	public CustomAsyncFunctonTuple3() {
	}

	@Override
	public void asyncInvoke(Tuple3<String, Integer, Long> input, ResultFuture<Tuple3<String, String, Long>> resultFuture) throws Exception {
		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://10.4.247.20:5432/apm_test";
		String username = "apm";
		String password = "apm";
		Connection connection= DriverManager.getConnection(url,username,password);
		Statement stmt = connection.createStatement();
		String s = sql + input.getField(1);
		ResultSet resultSet = stmt.executeQuery(s);
		String s1 = null;
		Tuple3 tuple3 = null;
		List list = new ArrayList<Tuple3>();
		if (resultSet == null){
			tuple3 = new Tuple3(input.getField(0),input.getField(1) + "没有",input.getField(2));
		}
		while (resultSet.next()){
			s1 = resultSet.getString("mapvalue");
 			if (s1 != null)break;
		}
		tuple3 = new Tuple3<>(input.getField(0),s1 == null ? "null" : s1,input.getField(2));
		list.add(tuple3);
		resultFuture.complete(list);
	}

	@Override
	public void timeout(Tuple3<String, Integer, Long> input, ResultFuture<Tuple3<String, String, Long>> resultFuture) throws Exception {

	}
}
