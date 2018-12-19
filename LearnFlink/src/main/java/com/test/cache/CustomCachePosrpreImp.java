package com.test.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;


import java.util.concurrent.TimeUnit;

public class CustomCachePosrpreImp implements CustomCache<String>{
	private transient Cache<String, String> cache;
//	private transient JDBCClient jdbcClient;
	private CustomCachePostpreConf customCacheConf;
	private String sql = null;

	public CustomCachePosrpreImp(CustomCachePostpreConf customCacheConf) throws Exception {
		this.customCacheConf = customCacheConf;
		sql = "select * from " + customCacheConf.getTableName();
		initDataBase();
		initCache();
	}

	private void initCache() {
//		cache = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.SECONDS).build(new CacheLoader<String, String>() {
//			public String load(String id) throws Exception
//			{
//				if (sql == null){
//					throw new Exception("cache sql 不能为null");
//				}
//				jdbcClient.getConnection(res ->{
//					if (res.succeeded()){
//						SQLConnection connection = res.result();
//						connection.query(sql,select -> {
//
//						});
//					}
//				});
//				return "User:" + id;
//			}
//		});
	}

	private void initDataBase() throws Exception {
//		JsonObject postgreSQLClientConfig = new JsonObject();
//		postgreSQLClientConfig.put("host",customCacheConf.getHost())
//			.put("port",customCacheConf.getPort())
//			.put("database",customCacheConf.getDatabase())
//			.put("username",customCacheConf.getUserName())
//			.put("password",customCacheConf.getPassword())
//			.put("database",customCacheConf.getTableName());
//		VertxOptions vo = new VertxOptions();
//		Vertx vertx = Vertx.vertx(vo);
//		AsyncSQLClientImpl asyncSQLClient = new AsyncSQLClientImpl(vertx,postgreSQLClientConfig,false);
	}

	@Override
	public Value getValue(String key) {
		return null;
	}
}
