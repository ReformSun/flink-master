package com.test.cache;

import java.util.Map;

/**
 * 直接使用postgre的异步客户端
 */
public interface PostgreConf {
	/**
	 * 设置数据库的连接信息
	 *  host", "localhost"
	 * "port",5432
	 * "database","apm_test"
	 * "charset","")
	 * "connectTimeout",""
	 * "testTimeout",""
	 * "queryTimeout",""
	 * "username",""
	 * "password",""
	 * "database",""
	 * "tablename",""
	 * @param config
	 */
	public void setPostgreConf(Map<String,Object> config);
	public String getHost();
	public String getDatabase();
	public String getUserName();
	public String getPassword();
	public int getPort() throws Exception;
	public String getTableName() throws Exception;
}
