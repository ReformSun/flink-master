package com.test.cache;

import java.util.Map;

/**
 * 数据库 普通端口 不知道某一个服务器
 */
public interface DataBaseConf {

	/**
	 * url string
	 * driver_class string
	 * user string
	 * password string
	 * max_pool_size int
	 * initial_pool_size int
	 * min_pool_size int
	 * max_statements int
	 * max_statements_per_connection int
	 * max_idle_time int
	 * acquire_retry_attempts int
	 * acquire_retry_delay int
	 * break_after_acquire_failure boolean
	 *
	 * @param config
	 */

	public void setDataBaseConf(Map<String,Object> config);
	public String getUrl();
	public String getDriver_class();
	public String getUser();
	public String getPassword();
	/**
	 * 得到join表的名字
	 * @param
	 */
	public String getTableName() throws Exception;
}
