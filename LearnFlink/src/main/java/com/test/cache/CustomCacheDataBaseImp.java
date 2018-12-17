package com.test.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.impl.AsyncSQLClientImpl;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.omg.CORBA.portable.CustomValue;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CustomCacheDataBaseImp implements CustomCache{

	private transient Cache<String, String> cache;
	private transient JDBCClient jdbcClient;
	private CustomCacheDataBaseConf customCacheConf;
	private String sql = null;
	private ExecutorService executor = Executors.newCachedThreadPool();

	public CustomCacheDataBaseImp(CustomCacheDataBaseConf customCacheConf) {
		this.customCacheConf = customCacheConf;
	}

	private void initDataBase() throws Exception {
		JsonObject sQLClientConfig = new JsonObject();
		sQLClientConfig.put(CommonValue.DATABASE_URL_KEY.getString(),customCacheConf.getUrl())
			.put(CommonValue.DRIVER_CLASS_KEY.getString(),customCacheConf.getDriver_class())
			.put(CommonValue.USER_KEY.getString(),customCacheConf.getUser())
			.put(CommonValue.PASSWORD_KEY.getString(),customCacheConf.getPassword());
		VertxOptions vo = new VertxOptions();
		Vertx vertx = Vertx.vertx(vo);
		jdbcClient = JDBCClient.createNonShared(vertx, sQLClientConfig);
	}

	private void initCache() {
		cache = CacheBuilder.newBuilder().refreshAfterWrite(5, TimeUnit.SECONDS).build(new CacheLoader<String, String>() {
			long time =0;
			@Override
			public String load(String key) throws Exception {
				System.out.println("查询数据库" + Thread.currentThread().getName());
				Thread.sleep(30000);
				return "dddd";
			}

			@Override
			public ListenableFuture<String> reload(String key, String oldValue) throws Exception {

				ListenableFuture<String> listenableFuture = Futures.submitAsync(() ->{
					String value = null;
					jdbcClient.getConnection(res -> {
						if (res.succeeded()){
							SQLConnection connection = res.result();
							connection.query(sql,select ->{
								if (select.failed()){

								}else {
									ResultSet resultSet = select.result();
									List<JsonObject> list = resultSet.getRows();
									for (JsonObject jsonObject:list){
										System.out.println(jsonObject.getString("mapvalue"));
									}

								}
							});
						}
					});
					return Futures.immediateFuture(value);
				},executor);
				return listenableFuture;
			}
		});
	}

	@Override
	public Value getValue(String key) {
		return null;
	}



}
