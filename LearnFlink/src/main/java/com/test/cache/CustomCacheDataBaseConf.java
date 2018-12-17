package com.test.cache;

import org.apache.flink.table.api.TableSchema;

import java.util.Map;

public class CustomCacheDataBaseConf implements DataBaseConf,TableConf {
	private Map<String,Object> databaseConf;
	private TableSchema tableSchema;
	private String keyName;
	@Override
	public void setDataBaseConf(Map<String, Object> config) {

	}

	@Override
	public String getUrl() {
		if (databaseConf != null){
			Object object = databaseConf.get(CommonValue.DATABASE_URL_KEY.getString());
			if (object == null)return CommonValue.DATABASE_URL.getString();
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.DATABASE_URL.getString();
	}

	@Override
	public String getDriver_class() {
		if (databaseConf != null){
			Object object = databaseConf.get(CommonValue.DRIVER_CLASS_KEY.getString());
			if (object == null)return CommonValue.DRIVER_CLASS.getString();
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.DRIVER_CLASS.getString();
	}

	@Override
	public String getUser() {
		if (databaseConf != null){
			Object object = databaseConf.get(CommonValue.USER_KEY.getString());
			if (object == null)return CommonValue.USER.getString();
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.USER.getString();
	}

	@Override
	public String getPassword() {
		if (databaseConf != null){
			Object object = databaseConf.get(CommonValue.PASSWORD_KEY.getString());
			if (object == null)return CommonValue.PASSWORD.getString();
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.PASSWORD.getString();
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
	public void setKeyName(String name) {
		this.keyName = name;
	}

	@Override
	public String getKeyName() {
		return keyName;
	}
}
