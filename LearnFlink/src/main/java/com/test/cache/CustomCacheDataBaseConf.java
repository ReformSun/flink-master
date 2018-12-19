package com.test.cache;

import org.apache.flink.table.api.TableSchema;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CustomCacheDataBaseConf implements DataBaseConf,KeysConf,CacheConf,Serializable {
	private Map<String,Object> databaseConf;
	private TableSchema tableSchema;
	private Key firstKey;
	private Key secondKey;
	@Override
	public void setDataBaseConf(Map<String, Object> config) {
		databaseConf = config;
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
	public String getTableName() throws Exception {
		if (databaseConf != null){
			Object object = databaseConf.get(CommonValue.TABLENAME_KEY.getString());
			if (object == null)throw new Exception(CommonValue.TABLENAME_KEY.getString() + " 键对应的值不能为空");
			if (object instanceof String)return (String)object;
			return object.toString();
		}
		return CommonValue.USERNAME.getString();
	}

	@Override
	public Key getFirstKey() {
		return firstKey;
	}

	@Override
	public Key getSecondKey() {
		return secondKey;
	}

	@Override
	public void setFirstKey(Key firstKey) {
		this.firstKey = firstKey;
	}

	@Override
	public void setSecondKey(Key secondKey) {
		this.secondKey = secondKey;
	}

	@Override
	public Long getRefreshInterval() throws Exception {
		if (databaseConf != null){
			Object object = databaseConf.get(CommonValue.INTERVAL_KEY.getString());
			if (object == null)throw new Exception(CommonValue.INTERVAL_KEY.getString() + " 键对应的值不能为空");
			if (object instanceof Long)return (Long) object;
			throw new Exception(CommonValue.INTERVAL_KEY.getString() + " 键对应的值类型不对，应为Long型");
		}
		return (Long) CommonValue.INTERVAL.getValue();
	}

	@Override
	public TimeUnit getRefreshIntervalUnit() {
		return (TimeUnit)CommonValue.UNIT.getValue();
	}
}
