package com.test.map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class CustomMapTuple3 extends RichMapFunction<Tuple3<String,Integer,Long>, Tuple3<String,String,Long>>{
	private static Statement  stmt;
	private String sql = "select mapvalue from \"public\".\"flink_map\" where mapname =";
	public CustomMapTuple3() {
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://10.4.247.20:5432/apm_test";
			String username = "apm";
			String password = "apm";
			Connection connection= DriverManager.getConnection(url,username,password);
			stmt = connection.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public Tuple3<String, String, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
		String s = sql + value.getField(1);
		ResultSet resultSet = stmt.executeQuery(s);
		String s1 = null;
		if (resultSet == null){
			return new Tuple3<>(value.getField(0),value.getField(1) + "没有",value.getField(2));
		}
		while (resultSet.next()){
			s1 = resultSet.getString("mapvalue");
			if (s1 != null)break;
		}
		return new Tuple3<>(value.getField(0),s1 == null ? "null" : s1,value.getField(2));
	}
}
