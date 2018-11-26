package com.test.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlinkJobManagerImp implements FlinkJobManager {
    private static Pattern pattern = Pattern.compile("[\\w-]+\\.jar{1}");
    private static Logger logger = Logger.getLogger(FlinkJobManagerImp.class);
    //    private final String baseUrl = "http://172.24.157.3:8081";
    private final String baseUrl = "http://10.4.247.17:8081";
//    private final String baseUrl = "http://172.31.35.58:8081";
//    private final String baseUrl = "http://172.31.35.58:8081";
//    private final String baseUrl = "http://10.4.251.99:8081";
//	private final String baseUrl = "http://localhost:8081";
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
    private static FlinkJobManagerImp instance;
    private FlinkJobManagerImp(){}

    public static FlinkJobManagerImp getInstance() {
        if (instance == null) {
            synchronized (FlinkJobManagerImp.class) {
                if (instance == null) {
                    instance = new FlinkJobManagerImp();
                }
            }
        }
        return instance;
    }

    @Override
    public ReadResult uploadJob(String jarPath, String jarName){
        String url= baseUrl + "/jars/upload";
        //创建连接客户端
        OkHttpClient client = new OkHttpClient();
        File file = new File(jarPath);
        if (file.exists()){
            RequestBody fileBody = RequestBody.create(MultipartBody.FORM,new File(jarPath));
            RequestBody requestBody = new MultipartBody.Builder().setType(MultipartBody.FORM).
                    addPart(Headers.of("Content-Disposition", "form-data; name=\"jarfile\"; filename=\"" + jarName + "\""),fileBody).build();
            Request request = new Request.Builder()
                    .url(url).post(requestBody)
                    .build();
            //创建"调用" 对象
            Call call = client.newCall(request);
            try {
                Response response = call.execute();

                ReadResult readResult = new ReadResult(response.code(),response.body().string());
                return readResult;
            } catch (IOException e) {
                logger.error("开始提交job jar包时出错",e);
                return new ReadResult(500,"开始提交job jar包时出错");
            }
        }else{
            logger.error("没有可操作main.jar包");
            return new ReadResult(500,"没有可操作main.jar包");
        }
    }


    /**
     *
     * @param jobName
     * @param entry_class
     * @param taskid
     * @param status 0 切分 1 数据集 2 报警 3 测试
     * @return
     * @throws UnsupportedEncodingException
     */
    @Override
    public ReadResult runJob(String jobName, String entry_class, String taskid, int status) throws UnsupportedEncodingException {

        if (jobName != null){



            String groupId = "serverCollector" + getUuid();
            Matcher matcher = pattern.matcher(jobName);
            if (matcher.find() && entry_class != null){
                String filename = matcher.group();
                String  u = baseUrl + "/jars/" + filename + "/run";
                Map<String,Object> map = new LinkedHashMap();
                map.put("entryClass",entry_class);
                if (status == 0){
                    map.put("programArgs","--bootstrap.servers" +
                            " 172.31.24.30:9092,172.31.24.36:9092" +
                            " --group.id " + groupId +
                            " --taskid " + taskid);

                }else if (status == 1){
                    map.put("programArgs","--bootstrap.servers" +
                            " 172.31.24.30:9092,172.31.24.36:9092" +
                            " --group.id " + groupId +
                            " --dataSetId " + taskid);
                }else {
//                    map.put("");
                    u = u + "?entry-class=" + entry_class;
                }

                return runFlinkJob(u,null);
            }else {
                return new ReadResult(500,"jobname错误");
            }
        }else {
            return new ReadResult(500,"数据库插入失败");
        }
    }


    @Override
    public ReadResult runAlarmJob(String jobName, String entry_class, Long alarmid, Long datasetId){
        String groupId = "serverCollector" + getUuid();
        Map<String,Object> map = new LinkedHashMap();
        map.put("entryClass",entry_class);
        map.put("programArgs","--input-topic monitorBlocklyQueueKeyAlarm" +
                " --bootstrap.servers" +
                " 172.31.24.30:9092,172.31.24.36:9092" +
                " --group.id " + groupId +
                " --alarmId " + alarmid +
                " --dataSetId " + datasetId);
        String u = baseUrl + "/jars/" + jobName +
                "/run";
        return runFlinkJob(u,gson.toJson(map));
    }

    @Override
    public boolean isRunningJob(String flinkJobId) {
        String u = baseUrl +  "/jobs/" + flinkJobId;
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(u).get()
                .build();
        Call call = client.newCall(request);
        try {
            Response response = call.execute();//执行
            if (response.code() == 404){
                return false;
            }else if (response.code() == 200){
                JSONObject jsonObject = JSONObject.parseObject(response.body().string());
                String state = jsonObject.getString("state");
                if (state != null){
                    if (state.equals("RUNNING")){
                        return true;
                    }else {
                        return false;
                    }
                }else {
                    return false;
                }
            }else {
                return false;
            }
        } catch (IOException e) {
            logger.error("获取job运行状态时出错",e);
            return false;
        }
    }

    @Override
    public ReadResult cancelJob(String flinkJobId) {
        String u = baseUrl +  "/jobs/" + flinkJobId +"/yarn-cancel";
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(u).build();
        Call call = client.newCall(request);
        try {
            Response response = call.execute();//执行
            ReadResult readResult = new ReadResult(response.code(),response.body().string());
            return readResult;
        } catch (IOException e) {
            logger.error("关闭job时出错",e);
            return new ReadResult(500,"关闭job时出错");
        }
    }

    /**
     *
     * @param jobName
     * @param entry_class
     * @param datasetMap
     * @return
     */
    @Override
    public ReadResult queryDatasetJob(String jobName, String entry_class, JSONObject datasetMap){

        if (jobName != null){
            Matcher matcher = pattern.matcher(jobName);
            if (matcher.find() && entry_class != null){
                String filename = matcher.group();
                StringBuilder flinkParameter=new StringBuilder();
                for (Map.Entry<String, Object> entry : datasetMap.entrySet()) {
                    flinkParameter.append(" --").append(entry.getKey()).append(" ").append(entry.getValue());
                }

                String u = baseUrl + "/jars/" + filename +
                        "/run";
                Map<String,Object> map = new LinkedHashMap();
                map.put("entryClass",entry_class);
                map.put("programArgs",flinkParameter.toString());

                return runFlinkJob(u,gson.toJson(map));

            }else {
                ReadResult readResult = new ReadResult(500,"没有指定启动的job任务名");
                return readResult;
            }
        }else {
            return new ReadResult(500,"数据库插入失败");
        }
    }


    private boolean insertTaskDetail(String taskId,String xmlContent,String filename){
        try{
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://10.4.247.20:5432/apm_test";
            String username = "apm";
            String password = "apm";
            Connection connection= DriverManager.getConnection(url,username,password);
            Statement stmt = connection.createStatement() ;
            String sqlStr = "INSERT INTO \"public\".test_xml(taskid,filename,xmlcontent) VALUES('" + taskId + "','"  + filename + "','" + xmlContent + "')";
            int i = stmt.executeUpdate(sqlStr);
            if (i == 1){
                return true;
            }
            connection.close();
            return false;
        }catch (SQLException|ClassNotFoundException s ){
            return false;
        }
    }


    private String getUuid(){
        UUID uuid = UUID.randomUUID();
        String str = uuid.toString();
        String uuidS = str.replace("-","");
        return uuidS;
    }

    @Override
    public ReadResult getJobsDetail(String flinkJobId) {
        String u = baseUrl + "/jobs/" + flinkJobId+"/metrics";
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(u).build();

        try {
            Response response = client.newCall(request).execute();
            String jsonData = response.body().string();
            JSONObject Jobject = JSONObject.parseObject(jsonData);
            JSONArray Jarray = Jobject.getJSONArray("vertices");
            JSONObject object = Jarray.getJSONObject(0);
            JSONObject metrics = object.getJSONObject("metrics");
            Integer readrecords = metrics.getInteger("read-records");
            ReadResult readResult = new ReadResult(response.code(),jsonData.toString());
            return readResult;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private ReadResult runFlinkJob(String url, String bodyS){
        RequestBody body = null;
        if (bodyS == null){
            body = new FormBody.Builder().build();
        }else {
            body =RequestBody.create(JSON,bodyS);
        }

        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.connectTimeout(2, TimeUnit.MINUTES);
        builder.readTimeout(2,TimeUnit.MINUTES);

        OkHttpClient client = builder.build();

        Request request = new Request.Builder()
                .url(url).header("Content-Type","application/json;charset=utf-8")
                .header("Accept","application/json, text/plain, */*").header("Accept-Encoding","gzip, deflate").post(body)
                .build();
        Call call = client.newCall(request);
        try {
            Response response = call.execute();
            ReadResult readResult = new ReadResult(response.code(),response.body().string());
            return readResult;
        } catch (IOException e) {
            logger.error("开始运行job时出错",e);
            ReadResult readResult = new ReadResult(500,e.toString());
            return readResult;
        }
    }

    @Override
    public ReadResult stopCluster() {
        return null;
    }

    @Override
    public ReadResult getConfig() {
        String url = baseUrl + "/config";
        Request request = new Request.Builder().url(url).build();
        return sendRequest(request);
    }

	@Override
	public ReadResult triggerSavepoints(String jobid) {
		String u = baseUrl + "/jobs/" + jobid+"/savepoints";
		Map<String,Object> map = new LinkedHashMap();
		map.put("target-directory","/root/savepoint");
		map.put("cancel-job",true);
		return sendRequest(getPostRequest(u,gson.toJson(map)));
	}

	@Override
	public ReadResult getSavepointStatus(String jobid, String triggerId) {
		String u = baseUrl + "/jobs/" + jobid+"/savepoints/" + triggerId;
		Request request = new Request.Builder().url(u).build();
		return sendRequest(request);
	}

	private ReadResult sendRequest(Request request){

        OkHttpClient client = new OkHttpClient();
        try {
            Response response = client.newCall(request).execute();
            ReadResult readResult = new ReadResult(response.code(),response.body().string());
            return readResult;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private Request getPostRequest(String url,String body){
		RequestBody requestBody =RequestBody.create(JSON,body);
		OkHttpClient.Builder builder = new OkHttpClient.Builder();
		builder.connectTimeout(2, TimeUnit.MINUTES);
		builder.readTimeout(2,TimeUnit.MINUTES);

		Request request = new Request.Builder()
			.url(url).header("Content-Type","application/json;charset=utf-8")
			.header("Accept","application/json, text/plain, */*").header("Accept-Encoding","gzip, deflate").post(requestBody)
			.build();
		return request;
	}

}
