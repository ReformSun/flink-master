package com.test.util;

import com.alibaba.fastjson.JSONObject;

import java.io.UnsupportedEncodingException;

public interface FlinkJobManager {
    public ReadResult uploadJob(String jarPath, String jarName);
    public ReadResult runJob(String jobName, String entry_class, String taskid, int status) throws UnsupportedEncodingException;
    public ReadResult runAlarmJob(String jobName, String entry_class, Long alarmid, Long datasetId);
    public ReadResult cancelJob(String flinkJobId);
    public boolean isRunningJob(String flinkJobId);
    public ReadResult getJobsDetail(String flinkJobId);
	ReadResult queryDatasetJob(String jobName, String entry_class, JSONObject datasetMap);
    public ReadResult stopCluster();
    public ReadResult getConfig();
    public ReadResult triggerSavepoints(String jobid);
	public ReadResult getSavepointStatus(String jobid,String triggerId);
	public ReadResult triggerRescaling(String jobid);
	public ReadResult getRescalingStatus(String jobid,String triggerId);
	public ReadResult getJobMetrics();
	public ReadResult getJobsOverview();

}
