package com.test.sink;

import com.test.util.PatternUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;

public class InfluxDBSink extends RichSinkFunction<Row> implements CheckpointedFunction {
    private  String openurl;
    private  String username;
    private  String password;
    private  String database;//数据库
    private  String measurement;//表名
    private Map<Integer,String> tagMap;
    private Map<Integer,String> fieldMap;
    private Integer time_index;
    private InfluxDB influxDB;
    private boolean flushOnCheckpoint;
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public InfluxDBSink(String openurl, String username, String password, String database, String measurement, Map<Integer, String> tagMap, Map<Integer, String> fieldMap, Integer time_index, boolean flushOnCheckpoint) {
        this.openurl = openurl;
        this.username = username;
        this.password = password;
        this.database = database;
        this.measurement = measurement;
        this.tagMap = tagMap;
        this.fieldMap = fieldMap;
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.time_index = time_index;
    }

    @Override
    public void invoke(Row value) throws Exception {

        Point.Builder builder = Point.measurement(measurement);
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();

        Iterator<Integer> iterator = tagMap.keySet().iterator();
        while (iterator.hasNext()){
            Integer integer = iterator.next();
            String string = tagMap.get(integer);
            Object object = value.getField(integer);
            tags.put(string,object instanceof String ? (String) object : String.valueOf(object));
        }

        Iterator<Integer> iterator2 = fieldMap.keySet().iterator();
        while (iterator2.hasNext()){
            Integer integer = iterator2.next();
            String string = fieldMap.get(integer);
            Object object = value.getField(integer);
            if (object instanceof Timestamp){
                Timestamp timestamp = (Timestamp)object;
                fields.put(string,timestamp.getTime());
            }else {
                fields.put(string,object);
            }
        }

        if (time_index < value.getArity()){
            Object object = value.getField(time_index);
            if (object instanceof String){
                String objectS = (String)object;
                Matcher matcher = PatternUtil.intgerPattern.matcher(objectS);
                if (matcher.find()){
                    Date date = sdf.parse(objectS);
                    builder.time(date.getTime(),TimeUnit.MILLISECONDS);
                }else {
                    builder.time(Long.valueOf(objectS),TimeUnit.MILLISECONDS);
                }
            }else if (object instanceof Long){
                builder.time((Long) object,TimeUnit.MILLISECONDS);
            }else if (object instanceof Timestamp){
                Timestamp timestamp = (Timestamp)object;
                builder.time(timestamp.getTime(),TimeUnit.MILLISECONDS);
            }else {
                throw new Exception("未知的时间对象类型" + object.getClass());
            }
        }else {
            throw new Exception("预聚合数据job时间索引：time_index " + time_index + "大于" + "row最大边界 " + value.getArity() );
        }
        builder.tag(tags);
        builder.fields(fields);
        influxDB.write("testDB", "", builder.build());

    }

    public static InfluxDBSinkBuilder builder(){
        return new InfluxDBSinkBuilder();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if(influxDB == null){
            influxDB = InfluxDBFactory.connect("http://"+openurl, username, "");
            int actions = 1000;
            int flushDuration = 30;
            TimeUnit flushDurationTimeUnit = TimeUnit.SECONDS;
            ThreadFactory threadFactory = Executors.defaultThreadFactory();
            influxDB.enableBatch(actions,flushDuration,flushDurationTimeUnit,threadFactory,new BiConsumerImp());
            influxDB.createDatabase(database);
        }
        super.open(parameters);
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in ElasticsearchSink.", cause);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkErrorAndRethrow();
        if (flushOnCheckpoint) {
            influxDB.flush();
            checkErrorAndRethrow();
        }
    }


    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    @Override
    public void close() throws Exception {
        if (influxDB != null)influxDB.close();
        super.close();
    }
    private static Object getValue(String blockType,Object value){
        switch (blockType){
            case "string" : return value instanceof String ? value : String.valueOf(value);
            case "long": return value instanceof Long ? value : Long.valueOf(String.valueOf(value));
            case "date": return value instanceof Long ? value : Long.valueOf(String.valueOf(value));
            case "double" : return value instanceof Double ? value : Double.valueOf(String.valueOf(value));
            default:return value;
        }
    }

    private class BiConsumerImp implements BiConsumer<Iterable<Point>,Throwable> {
        @Override
        public void accept(Iterable<Point> points, Throwable throwable) {
            failureThrowable.getAndSet(throwable);
        }
    }


	public static class InfluxDBSinkBuilder {
		private String database;//数据库
		private String measurement;
		private Map<Integer, String> tagMap;
		private Map<Integer, String> fieldMap;
		private boolean flushOnCheckpoint = false;
		private Integer time_index;
		private Properties influxDBprops;

		public InfluxDBSinkBuilder setTime_index(Integer time_index) {
			this.time_index = time_index;
			return this;
		}

		public InfluxDBSinkBuilder withInfluxDBProperties(Properties props) {
			Preconditions.checkNotNull(props, "Properties must not be null.");
			Preconditions.checkArgument(this.influxDBprops == null, "Properties have already been set.");
			this.influxDBprops = props;
			return this;
		}

//    public InfluxDBSinkBuilder setOpenurl(String openurl) {
//        this.openurl = openurl;
//        return this;
//    }
//
//    public InfluxDBSinkBuilder setUsername(String username) {
//        this.username = username;
//        return this;
//    }
//
//    public InfluxDBSinkBuilder setPassword(String password) {
//        this.password = password;
//        return this;
//    }

		public InfluxDBSinkBuilder setDatabase(String database) {
			this.database = database;
			return this;
		}

		public InfluxDBSinkBuilder setMeasurement(String measurement) {
			this.measurement = measurement;
			return this;
		}

		public InfluxDBSinkBuilder setTagMap(Map<Integer, String> tagMap) {
			this.tagMap = tagMap;
			return this;
		}

		public InfluxDBSinkBuilder setFieldMap(Map<Integer, String> fieldMap) {
			this.fieldMap = fieldMap;
			return this;
		}

		public InfluxDBSinkBuilder setFlushOnCheckpoint(boolean flushOnCheckpoint) {
			this.flushOnCheckpoint = flushOnCheckpoint;
			return this;
		}

		public InfluxDBSink build() {

			Preconditions.checkNotNull(influxDBprops.getProperty("influxDBUrl"), "url 不能为null");
			Preconditions.checkNotNull(influxDBprops.getProperty("username"), "用户名不能为null");
//        Preconditions.checkNotNull(influxDBprops.getProperty("password"),"数据库密码不能为null");
			Preconditions.checkNotNull(database, "数据库名不能为null");
			Preconditions.checkNotNull(measurement, "表名不能为null");
			Preconditions.checkNotNull(fieldMap, "测量值不能为null");
			Preconditions.checkNotNull(time_index, "时间位置索引不能为null");

			return new InfluxDBSink(influxDBprops.getProperty("influxDBUrl"), influxDBprops.getProperty("username"), influxDBprops.getProperty("password"), database, measurement, tagMap, fieldMap, time_index, flushOnCheckpoint);
		}
	}

}
