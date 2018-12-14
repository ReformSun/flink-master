package com.test.cache;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.In;

import java.util.Map;

public class CustomCacheConf implements PostgreConf,TableConf{

	private Map<String,Object> postgreConf;
	private TableSchema tableSchema;
	private String keyName;
	@Override
	public void setPostgreConf(Map<String, Object> config) {
		postgreConf = config;
	}

	@Override
	public String getHost(){
		if (postgreConf != null){
			Object object = postgreConf.get(CommonValue.HOST_KEY.getString());
			if (object == null)return CommonValue.HOST.getString();
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.HOST.getString();
	}

	@Override
	public String getDatabase(){
		if (postgreConf != null){
			Object object = postgreConf.get(CommonValue.DATABASE_KEY.getString());
			if (object == null)return CommonValue.DATABASE.getString();
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.DATABASE.getString();
	}

	@Override
	public String getUserName(){
		if (postgreConf != null){
			Object object = postgreConf.get(CommonValue.USERNAME_KEY.getString());
			if (object == null)return CommonValue.USERNAME.getString();
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.USERNAME.getString();
	}

	@Override
	public String getPassword(){
		if (postgreConf != null){
			Object object = postgreConf.get(CommonValue.PASSWORD_KEY.getString());
			if (object == null)return CommonValue.PASSWORD.getString();
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.PASSWORD.getString();
	}

	@Override
	public int getPort() throws Exception {
		if (postgreConf != null){
			Object object = postgreConf.get(CommonValue.PORT_KEY.getString());
			if (object == null)return CommonValue.PORT.getInt();
			if (object instanceof Integer)return (int)object;
			throw new Exception(CommonValue.PORT_KEY.getString() + " 不是int类型");
		}
		return CommonValue.PORT.getInt();
	}

	@Override
	public String getTableName() throws Exception {
		if (postgreConf != null){
			Object object = postgreConf.get(CommonValue.TABLENAME_KEY.getString());
			if (object == null)throw new Exception(CommonValue.TABLENAME_KEY.getString() + " 键对应的值不能为空");
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.USERNAME.getString();
	}

	@Override
	public void setTableSchema(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}
	@Override
	public String getKeyName() {
		return keyName;
	}
}
