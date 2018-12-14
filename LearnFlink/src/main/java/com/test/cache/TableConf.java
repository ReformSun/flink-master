package com.test.cache;

import org.apache.flink.table.api.TableSchema;

public interface TableConf {
	public void setTableSchema(TableSchema tableSchema);
	public TableSchema getTableSchema();
	public void setKeyName(String name);
	public String getKeyName();
}
