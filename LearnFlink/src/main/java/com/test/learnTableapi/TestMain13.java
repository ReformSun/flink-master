package com.test.learnTableapi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestMain13 {
	public static void main(String[] args) throws Exception {
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		String datasetDataSql = "",socketId = "",tabSql = "",datasetId = "";
		String chartId="0";
		Map<String, TypeInformation> columnMap = new LinkedHashMap<>();
		Map<String, TypeInformation> outColumnMap = new LinkedHashMap<>();
		if (parameterTool.get("datasetqueryid") != null){
			long datasetqueryid = Long.valueOf(parameterTool.get("datasetqueryid"));
			//long datasetqueryid = 284;
			try{
				Class.forName("org.postgresql.Driver");
				String url = "jdbc:postgresql://10.4.247.20:5432/gpexmp";
				String username = "apm";
				String password = "apm";
				Connection connection= DriverManager.getConnection(url,username,password);
				Statement stmt = connection.createStatement() ;
				String sqlStr = "SELECT tabsql,datasetdatasql,fieldname,fieldnametype,datasetid FROM \"public\".datasetquery WHERE datasetquery_id =  "+ datasetqueryid ;
				ResultSet rs = stmt.executeQuery(sqlStr);

				if (rs.next()){
					datasetDataSql = rs.getString("datasetdatasql");
					//数据源SQL
					tabSql = rs.getString("tabsql");
					//表名
					datasetId = rs.getString("datasetid");
					columnMap = processingQueryResults(rs.getString("fieldnametype"));
					outColumnMap = processingQueryResults(rs.getString("fieldname"));
				}else {
					System.out.println("datasetqueryid：" + datasetqueryid + "没有相对应的查询数据集信息");
					return;
				}
				connection.close();
			}catch (SQLException |ClassNotFoundException s){
				s.printStackTrace();
			}
		}else
		{
			System.out.println("Mig parameters!\n" +"Usage: --datasetqueryid <some id>");
			return;
		}

		if (parameterTool.get("socketid") != null){
			socketId =parameterTool.get("socketid");
		}else
		{
			System.out.println("Mig parameters!\n" +"Usage: --socketid <some id>");
			return;
		}

		if (parameterTool.get("chartId") != null){
			chartId =parameterTool.get("chartId");
		}else
		{
			//System.out.println("Mig parameters!\n" +"Usage: --chartId <some id>");
			//return;
			chartId="0";
		}

		String[] outFieldNames = new String[outColumnMap.size()];
		TypeInformation[] outTypeInformations = new TypeInformation[outColumnMap.size()];
		Iterator<String> outIterator = outColumnMap.keySet().iterator();
		int j = 0;
		while (outIterator.hasNext()){
			String fieldName = outIterator.next();
			outFieldNames[j] = fieldName;
			TypeInformation typeInformation = outColumnMap.get(fieldName);
//			if (typeInformation.toString().equals("Long")){
//				typeInformation = Types.INT;
//			}
			outTypeInformations[j] = typeInformation;
			j ++;
		}
		final RowTypeInfo outRowTypeInfo = new RowTypeInfo(outTypeInformations,outFieldNames);

		String[] fieldNames = new String[columnMap.size()];
		TypeInformation[] typeInformations = new TypeInformation[columnMap.size()];
		Iterator<String> iterator = columnMap.keySet().iterator();
		int i = 0;
		while (iterator.hasNext()){
			String fieldName = iterator.next();
			fieldNames[i] = fieldName;
			TypeInformation typeInformation = columnMap.get(fieldName);
			typeInformations[i] = typeInformation;
			i ++;
		}

		final RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations,fieldNames);

		//Creates an execution environment that represents the context in which the program is currently executed.
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(environment);
		String driverName = "org.postgresql.Driver";
		String sourceDB = "gpexmp";
		//String sinkDB = "gpexmp";
		String dbURL = "jdbc:postgresql://10.4.247.20:5432/";
		String dbPassword = "apm";
		String dbUser = "apm";

		//Define Input Format Builder
		JDBCInputFormat.JDBCInputFormatBuilder inputBuilder =
			JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(driverName)
				.setDBUrl(dbURL + sourceDB)
				.setQuery(tabSql)
				.setRowTypeInfo(rowTypeInfo)
				.setUsername(dbUser)
				.setPassword(dbPassword);

		//Get Data from SQL Table
		DataSet<Row> source = environment.createInput(inputBuilder.finish());
		//Print DataSet
		tableEnv.registerDataSetInternal("tableName", source);
		Table result=tableEnv.sqlQuery(datasetDataSql);

		try {
			String kafkaoutputtopic="FlinkReturnDataSet";
			DataSet<Row> dataSet = tableEnv.toDataSet(result, Row.class);
			if(parameterTool.get("kafkaoutputtopic") != null)
				kafkaoutputtopic=parameterTool.get("kafkaoutputtopic");
			dataSet.print();
		} catch (Exception e) {
			e.printStackTrace();
		}

		//Triggers the program execution.
		try {
			environment.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private static Map<String, TypeInformation> processingQueryResults(String jString) throws SQLException {
		Map<String, TypeInformation> map = new LinkedHashMap<>();
		Gson gson = new GsonBuilder().serializeNulls().enableComplexMapKeySerialization().create();
		Type type = new TypeToken<Map<String, Object>>(){}.getType();
		Map<String, Object> innerjsonMap = gson.fromJson(jString, type);
		for (String keyString : innerjsonMap.keySet()) {
			String value=innerjsonMap.get(keyString).toString();
			map.put(keyString,getTypeInformation(value));
		}
		return map;
	}


	private static TypeInformation getTypeInformation(String blockType){
		switch (blockType){
			case "string" : return org.apache.flink.api.common.typeinfo.Types.STRING;
			case "long": return org.apache.flink.api.common.typeinfo.Types.LONG;
			case "date": return org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;
			case "double" : return org.apache.flink.api.common.typeinfo.Types.DOUBLE;
			default:return Types.STRING;
		}
	}
}
