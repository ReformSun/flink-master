package com.test.cache;

import java.util.concurrent.TimeUnit;

public interface CacheConf {
	/**
	 * refreshinterval
	 * 单位默认为毫秒
	 * @return
	 */
	public Long getRefreshInterval() throws Exception;
	public TimeUnit getRefreshIntervalUnit();
}
