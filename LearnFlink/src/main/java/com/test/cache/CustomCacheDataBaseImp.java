package com.test.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CustomCacheDataBaseImp<T> implements CustomCache<T>,Serializable{

	private transient LoadingCache<T, Value> cache;
//	private transient JDBCClient jdbcClient;
	private transient DataSource dataSource;
	private CustomCacheDataBaseConf customCacheConf;
	private String sql = null;
	private static ExecutorService executor = Executors.newCachedThreadPool();

	public static CustomCacheDataBaseImp creatCache(CustomCacheDataBaseConf customCacheConf) throws Exception {
		switch (customCacheConf.getFirstKey().getTypeInformation()){
			case "string" : return new CustomCacheDataBaseImp<String>(customCacheConf);
			case "long": return new CustomCacheDataBaseImp<Long>(customCacheConf);
			case "date": return new CustomCacheDataBaseImp<Long>(customCacheConf);
			case "double" : return new CustomCacheDataBaseImp<Double>(customCacheConf);
			default:throw new Exception("未知的数据类型");
		}
	}

	private CustomCacheDataBaseImp(CustomCacheDataBaseConf customCacheConf) throws Exception {
		this.customCacheConf = customCacheConf;
//		initDataBase();
//		initCache();
		sql = "select " + customCacheConf.getSecondKey().getKeyName() + " from " + customCacheConf.getTableName() + " where " + customCacheConf.getFirstKey().getKeyName() + " = ";
	}

	public void initDataBase() throws Exception {
		JsonObject sQLClientConfig = new JsonObject();
		sQLClientConfig.put(CommonValue.DATABASE_URL_KEY.getString(),customCacheConf.getUrl())
			.put(CommonValue.DRIVER_CLASS_KEY.getString(),customCacheConf.getDriver_class())
			.put(CommonValue.USER_KEY.getString(),customCacheConf.getUser())
			.put(CommonValue.PASSWORD_KEY.getString(),customCacheConf.getPassword());

//		jdbcClient = JDBCClient.createNonShared(vertx, sQLClientConfig);
		C3P0DataSourceProvider c3P0DataSourceProvider = new C3P0DataSourceProvider();
		dataSource = c3P0DataSourceProvider.getDataSource(sQLClientConfig);

	}

	public void initCache() throws Exception {
		cache = CacheBuilder.newBuilder().refreshAfterWrite(customCacheConf.getRefreshInterval(),customCacheConf.getRefreshIntervalUnit()).build(new CacheLoader<T, Value>() {
			long time =0;
			@Override
			public Value load(T key) throws Exception {
				System.out.println(Thread.currentThread().getName() +" load");
				return getValueFromDateBase(key);
			}

			@Override
			public ListenableFuture<Value> reload(T key, Value oldValue) throws Exception {
//				ListenableFuture<Value> listenableFuture = Futures.submitAsync(() ->{
//					return Futures.immediateFuture(getValueFromDateBase(key));
//			},executor);
//				return listenableFuture;
				return null;
			}

		});
	}

	private Value getValueFromDateBase(T key) throws Exception {
//		final Object[] value = new Object[3];
//		jdbcClient.getConnection(res -> {
//			if (res.succeeded()){
//				SQLConnection connection = res.result();
//				String keysql  = null;
//				if (customCacheConf.getFirstKey().getTypeInformation().equals("String")){
//					keysql = sql + "'" + key + "'";
//				}else if (customCacheConf.getFirstKey().getTypeInformation().equals("long") || customCacheConf.getFirstKey().getTypeInformation().equals("double")){
//					keysql = sql + key ;
//				}
//				connection.query(keysql,select ->{
//					if (select.failed()){
//						value[2] = new Exception("查询错误",select.cause());
//					}else {
//						ResultSet resultSet = select.result();
//						List<JsonObject> list = resultSet.getRows();
//						if (list.size() != 1){
//							value[2] = new Exception("查询结果有误",select.cause());
//						}else {
//							JsonObject jsonObject = list.get(0);
//							value[0] = jsonObject.getValue(customCacheConf.getSecondKey().getKeyName());
//							value[1] = customCacheConf.getSecondKey().getTypeInformation();
//						}
//
//					}
//				});
//			}else {
//				value[2] = new Exception("连接错误",res.cause());
//			}
//		});
//		if (value[2] != null){
//			throw (Exception) value[2];
//		}
//		return new Value(value[0],(String) value[1]);

		String keysql  = null;
		if (customCacheConf.getFirstKey().getTypeInformation().equals("String")){
			keysql = sql + "'" + key + "'";
		}else if (customCacheConf.getFirstKey().getTypeInformation().equals("long") || customCacheConf.getFirstKey().getTypeInformation().equals("double")){
			keysql = sql + key ;
		}
		Connection connection =dataSource.getConnection();
		Statement statement = connection.createStatement();
		java.sql.ResultSet resultSet = statement.executeQuery(keysql);
		if (resultSet != null){
			while (resultSet.next()){
				return new Value(resultSet.getObject(customCacheConf.getSecondKey().getKeyName()),customCacheConf.getSecondKey().getTypeInformation());
			}
			throw new Exception("没有查找到key= "+ key +"的值");
		}else {
			throw new Exception("没有查找到key= "+ key +"的值");
		}

	}

	@Override
	public Value getValue(T key) throws ExecutionException {
		return cache.get(key);
	}



}
