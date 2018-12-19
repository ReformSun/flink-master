package com.test.cache;

import org.apache.flink.table.api.TableSchema;

public interface TableConf {
	/**
	 * 设置要join的表的字段信息 包含字段名和字段类型
	 * @param tableSchema
	 */
	public void setTableSchema(TableSchema tableSchema);
	public TableSchema getTableSchema();

	/**
	 * 设置要join的字段名，就是表中的key值
	 * @param name
	 */
	public void setKeyName(String name);
	public String getKeyName();
}
