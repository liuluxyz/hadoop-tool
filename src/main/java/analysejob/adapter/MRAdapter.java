package analysejob.adapter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.util.XMLUtils;

import analysejob.AnalyseJobOption;
import analysejob.common.job.JobConfKey;
import analysejob.common.job.MRBean;
import analysejob.common.job.TaskReportUDF;

import common.JobConfParser;

/**
 * 适配器
 * @author yangjp
 *
 */
public class MRAdapter {
	
	private static final Log log = LogFactory.getLog(MRAdapter.class);
	
	JobClient jobClient = null;
	FileSystem fs;
	
	public MRAdapter() throws Exception{
		if(jobClient == null) {
			jobClient = new JobClient(new Configuration());
			if(jobClient == null){
				throw new Exception("not find mapred-site.xml or error file");
			}
		}
		if(fs == null){
			fs = FileSystem.get(new Configuration());
		}
	}
	
	public MRAdapter(String conf) throws Exception{
		if(jobClient == null) {
			jobClient = new JobClient(new JobConf(conf));
			if(jobClient == null){
				throw new Exception("not find mapred-site.xml or error file");
			}
		}
		if(fs == null){
			Configuration config = new Configuration();
			config.addResource(conf);
			fs = FileSystem.get(config);
		}
	}
	
	public MRBean getMRInfoByJobid(String jobId) throws IOException {
		JobStatus[] jobs = jobClient.getAllJobs();
		for(JobStatus job : jobs){
			if(job.getJobID().toString().equals(jobId)){
				return getMRInfoByJobid(job.getJobID());
			}
		}
		return null;
	}
	
	/**
	 * 根据jobId 得到 MRBean
	 * @param jobId
	 * @return
	 * @throws IOException
	 */
	public MRBean getMRInfoByJobid(JobID jobId) throws IOException {
		MRBean mrBean = new MRBean();
		RunningJob rjob = jobClient.getJob(jobId);
		
		JobStatus job = rjob.getJobStatus();
		//填充counter
		Counters cs = rjob.getCounters();
		fullCounters(cs,mrBean);
		
		//填充counter over
		
		// 传入jobId
		mrBean.setJobId(job.getJobId());
		mrBean.setJobName(jobClient.getJob(job.getJobID()).getJobName());
		mrBean.setJobStartTime(job.getStartTime());
		long endTime = 0;


		TaskReport[] maps = jobClient.getMapTaskReports(job.getJobID());

		List<TaskReportUDF> mapList = mrBean.getMaps();
		TaskReportUDF trudf = null;
		for (TaskReport tr : maps) {
			trudf = new TaskReportUDF();
			
			trudf.setStatus(tr.getCurrentStatus());
			
			trudf.setStartTime(tr.getStartTime());
			long finishTime = tr.getFinishTime();
			if (finishTime > endTime) {
				endTime = finishTime;
			}
			trudf.setFinishTime(finishTime);
			trudf.setTaskid("" + tr.getTaskID().getId());
			
			fullTaskCounters(trudf, tr);
			
			mapList.add(trudf);
		}
		mrBean.setMapNums(maps.length);

		TaskReport[] reduces = jobClient.getReduceTaskReports(job.getJobID());

		List<TaskReportUDF> reduceList = mrBean.getReduces();

		for (TaskReport tr : reduces) {
			trudf = new TaskReportUDF();
			trudf.setStatus(tr.getCurrentStatus());
			trudf.setStartTime(tr.getStartTime());
			long finishTime = tr.getFinishTime();
			if (finishTime > endTime) {
				endTime = finishTime;
			}
			trudf.setFinishTime(finishTime);
			trudf.setTaskid("" + tr.getTaskID().getId());
			
			fullTaskCounters(trudf, tr);
			
			reduceList.add(trudf);
		}
		mrBean.setReduceNums(reduces.length);

		// TODO 设置job的结束时间
		mrBean.setJobEndTime(endTime);
		
		mrBean.setJobRunStates(job.getRunState());
		
		Map<String, String> jobConf = parseJobConf(jobId);
//		log.info("get jobconf num : " + jobConf.size() + " [" + jobId + "]");
		mrBean.setConf(jobConf);
		
		return  mrBean;
	}
	/**
	 * 填充task的计数器
	 * @param trudf
	 * @param tr
	 */
	private void fullTaskCounters(TaskReportUDF trudf, TaskReport tr) {
		Counters cs;
		cs =  tr.getCounters();
		Iterator<String> is = cs.getGroupNames().iterator();
		Map<String,Long> anyMap = null; 
		while(is.hasNext()){
			String s = is.next();
			if(log.isDebugEnabled()){
				log.debug("task group name : " + s);
			}
			if(s.equals("org.apache.hadoop.mapreduce.FileSystemCounter")) {
				anyMap = trudf.getFsCounter();
			}else 
			if(s.equals("org.apache.hadoop.mapreduce.TaskCounter")) {
				anyMap = trudf.getTaskCounter();
			}
			
			Iterator<Counter> iss = cs.getGroup(s).iterator();
			while(iss.hasNext()) {
				Counter c = iss.next();
				anyMap.put(c.getName(), c.getCounter());
				if(log.isDebugEnabled()){
					log.debug("counter : [" + c.getName() + "][" + c.getCounter() + "]");				
				}
			}
			if(log.isDebugEnabled()){
				log.debug("task group counter name : " + s + ", num : " + anyMap.size());
			}
		}
	}
	/**
	 * 
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public List<MRBean> getMRInfo() throws IOException {
		JobStatus[] jobs = jobClient.getAllJobs();
		List<MRBean> mrList = new ArrayList<MRBean>();
		for (JobStatus job : jobs) {
			if(job.getRunState() == JobStatus.RUNNING || job.getRunState() == JobStatus.SUCCEEDED){
				mrList.add(getMRInfoByJobid(job.getJobID()));
			}
			
		}
		return mrList;
	}

	/**
	 * 根据jobname获取job信息
	 * @param jobname
	 * @param match
	 * @return
	 * @throws IOException
	 */
	public List<MRBean> getMRInfoByJobname(String jobname, AnalyseJobOption.Match match) throws IOException {
		JobStatus[] jobs = jobClient.getAllJobs();
		List<MRBean> mrList = new ArrayList<MRBean>();
		for (JobStatus job : jobs) {
			RunningJob rjob = jobClient.getJob(job.getJobID());
			if(match.ordinal() == AnalyseJobOption.Match.equal.ordinal()){
				if(rjob.getJobName().equals(jobname)){
					mrList.add(getMRInfoByJobid(job.getJobID()));
				}
			}else{
				if(rjob.getJobName().contains(jobname)){
					mrList.add(getMRInfoByJobid(job.getJobID()));
				}
			}
		}
		return mrList;
	}
	
	/**
	 * 根据hiveql匹配【hive.query.string】获取job
	 * @param hiveql
	 * @param match
	 * @return
	 * @throws IOException
	 */
	public List<MRBean> getMRInfoByHiveql(String hiveql, AnalyseJobOption.Match match) throws IOException {
		JobStatus[] jobs = jobClient.getAllJobs();
		List<MRBean> mrList = new ArrayList<MRBean>();
		for (JobStatus job : jobs) {
			Map<String, String> jobConf = this.parseJobConf(job.getJobID());
			
			String hiveQueryString = jobConf.get(JobConfKey.hiveQueryString);
			if(hiveQueryString == null){
//				log.info("find no hive.query.string : " + job.getJobId());
				continue;
			}
			if(match.ordinal() == AnalyseJobOption.Match.equal.ordinal()){
				if(hiveQueryString.equals(hiveql)){
					mrList.add(getMRInfoByJobid(job.getJobID()));
				}
			}else{
				if(hiveQueryString.contains(hiveql)){
					mrList.add(getMRInfoByJobid(job.getJobID()));
				}
			}
		}
		return mrList;
	}
	
	/**
	 * 填充counters
	 * @param cs
	 * @param mrBean
	 */
	private static void fullCounters(Counters cs, MRBean mrBean) {
		Iterator<String> is = cs.getGroupNames().iterator();
		Map<String,Long> anyMap = null; 
		while(is.hasNext()){
			String s = is.next();
			if(log.isDebugEnabled()){
				log.debug("counter group name : " + s);				
			}
			if(s.equals("org.apache.hadoop.mapreduce.FileSystemCounter")) {
				anyMap = mrBean.getFsCounter();
			}else 
			if(s.equals("org.apache.hadoop.mapreduce.JobCounter")) {
				anyMap = mrBean.getJobCounter();
			}else 
			if(s.equals("org.apache.hadoop.mapreduce.TaskCounter")) {
				anyMap = mrBean.getTaskCounter();
			}else 
			if(s.equals("org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter")) {
				anyMap = mrBean.getFifCounter();
			}
			Iterator<Counter> iss = cs.getGroup(s).iterator();
			while(iss.hasNext()) {
				Counter c = iss.next();
				anyMap.put(c.getName(), c.getCounter());
				if(log.isDebugEnabled()){
					log.debug("counter : [" + c.getName() + "][" + c.getCounter() + "]");				
				}
			}
		}
	}
	
	/**
	 * 解析job的conf信息
	 * @param rjob
	 * @return
	 */
	private Map<String, String> parseJobConf(JobID jobid){
		try {
			RunningJob rjob = jobClient.getJob(jobid);
			log.info("job state : [" + rjob.getJobID() + "] [" + JobStatus.getJobRunState(rjob.getJobState()) + "]");

			if(MRConfig.YARN_FRAMEWORK_NAME.equals(jobClient.getConf().get(MRConfig.FRAMEWORK_NAME))){
				return JobConfParser.parse(rjob.getJobFile(), false, fs);
			}
			else{//mr1
				if(rjob.getJobState() == JobStatus.RUNNING){
					return JobConfParser.parse(rjob.getJobFile(), false, fs);
				}
				else if(rjob.getJobState() == JobStatus.SUCCEEDED){
					
					/**
					 * mr1与yarn的api不兼容：此处需要hadoop-core-2.3.0-mr1-cdh5.0.0.jar中的JobTracker.java
					 */
//					String jobFilePath = JobTracker.getLocalJobFilePath(rjob.getID());
//					if(new File(jobFilePath).exists()){//非history任务
////						log.info("success job file : [" + rjob.getJobID() + "] [" + jobFilePath + "]");
//						return JobConfParser.parse(jobFilePath, true, null);
//					}else{
////						log.warn("job file not exist: [" + rjob.getJobID() + "] [" + jobFilePath + "]");
//					}
//					String jobHisFilePath = JobHistory.getHistoryFilePath(rjob.getID());//history任务
//					if(jobHisFilePath == null){
//						log.info("history job file not found: [" + rjob.getJobID() + "]");
//						return new HashMap<String, String>();
//					}
//					log.info("history job file : [" + rjob.getJobID() + "] [" + jobHisFilePath + "]");
//					return JobConfParser.parse(jobHisFilePath, true, null);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new HashMap<String, String>();
	}
	
	/**
	 * 获得conf的属性
	 * @param conf
	 * @param key
	 * @return
	 */
//	public String getConf(String key) {
//		return jobClient.getConf().get(key);
//	}
}
