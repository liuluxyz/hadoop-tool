package test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;

import common.JobConfParser;


/**
 * liulu5
 * 2013-11-27
 */
public class Test {

	private static void testJob(){
		String conf = "D:\\workspace\\workspace_tools\\hadoop_tool\\src\\test\\mapred-site.xml";
		try {
			JobClient jobClient = new JobClient(new JobConf(conf));
			
			RunningJob job1 = jobClient.getJob(JobID.forName("job_201311251643_0555"));
			System.out.println(JobStatus.getJobRunState(job1.getJobState()));
			
			RunningJob job2 = jobClient.getJob(JobID.forName("job_201311251643_0554"));
			System.out.println(JobStatus.getJobRunState(job1.getJobState()));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private static void testParseXML(){
		String conf = "D:\\workspace\\workspace_tools\\hadoop_tool\\src\\test\\mapred-site.xml";
		try {
			Map<String, String> maps = JobConfParser.parse(conf, true, null);
			
			System.out.println(maps.size());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	private static void testFileStatus(String filepath){
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus stat = fs.getFileStatus(new Path(filepath));
			System.out.println("len : " + stat.getLen());
			
			BlockLocation[] locations = fs.getFileBlockLocations(stat, 0, stat.getLen());
			System.out.println("locations num : " + locations.length);
			for(BlockLocation loc : locations){
				System.out.println("blocklocation size : " + loc.getLength());
				
				System.out.println("loc.getHosts : " + Arrays.toString(loc.getHosts()));
				
				System.out.println("loc.getTopologyPaths : " + Arrays.toString(loc.getTopologyPaths()));
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void testJobhistory() throws Exception  {
		Configuration conf = new Configuration();
//		conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\analyse\\mapred-site.xml"));
		conf.addResource(new Path("/home/liulu/app/hadoop/etc/hadoop/mapred-site.xml"));
		JobHistory his = new JobHistory();
		his.init(conf);
		
		Map<JobId, Job> jobs = his.getAllJobs();
		System.out.println("job num : " + jobs.size());
		Iterator<JobId> it = jobs.keySet().iterator();
		while(it.hasNext()){
			JobId jobId = it.next();
			System.out.println("jobid : " + jobId.toString());
			Job job = jobs.get(jobId);
			System.out.println("-------------PartialJob-----------");
			System.out.println("job class : " + job.getClass().toString());
			System.out.println("id : " + jobId.getId());
			System.out.println(job.getName());
			
			System.out.println("-------------CompletedJob-----------");
			Job fullJob = his.getJob(jobId);
			System.out.println("fullJob class : " + fullJob.getClass().toString());
			Counters countersFromFull = fullJob.getAllCounters();
			System.out.println("countersFromFull num : " + countersFromFull.countCounters());
			Map<TaskId,Task> tasks = fullJob.getTasks();
			System.out.println("fullJob get task num : " + tasks.size());
			
			System.out.println("==================================");
		}
		
		
		his.close();
	}
	
	private static void testHistoryFile(Configuration conf, JobId jobId) throws IOException{
		HistoryFileManager ma = new HistoryFileManager();
		ma.init(conf);
		Collection<HistoryFileInfo> allFileInfos = ma.getAllFileInfo();
		for(HistoryFileInfo info : allFileInfos){
			System.out.println("test HistoryFileInfo : " + info.getJobId());
			Job job = info.loadJob();
			Counters counters = job.getAllCounters();
			if(counters == null){
				System.out.println("counters is null in history test...");
			}else{
				System.out.println("counters num : " + counters.countCounters());
			}
		}
		
//		HistoryFileInfo fileInfo = ma.getFileInfo(jobId);
//		Job job = fileInfo.loadJob();
//		Counters counters = job.getAllCounters();
//		if(counters == null){
//			System.out.println("counters is null in history test...");
//		}else{
//			System.out.println("counters num : " + counters.countCounters());
//		}
	}
	
	private static void printCounter(Counters counters){
		Iterator<CounterGroup> it = counters.iterator();
		while(it.hasNext()){
			CounterGroup group = it.next();
			System.out.println("group name : " + group.getName());
			Iterator<Counter> counterIt = group.iterator();
			while(counterIt.hasNext()){
				Counter cou = counterIt.next();
				System.out.println("	" + cou.getName() + "" + cou.getValue());
			}
			
		}
	}
	private static void printCounter(org.apache.hadoop.mapred.Counters counters){
		Iterator<Group> it = counters.iterator();
		while(it.hasNext()){
			Group group = it.next();
			System.out.println("group name : " + group.getName());
			Iterator<org.apache.hadoop.mapred.Counters.Counter> counterIt = group.iterator();
			while(counterIt.hasNext()){
				Counter cou = counterIt.next();
				System.out.println("	" + cou.getName() + " : " + cou.getValue());
			}
			
		}
	}
	
	private static void testJobClient(){
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\analyse\\mapred-site.xml"));
			conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\analyse\\yarn-site.xml"));
//			conf.addResource(new Path("/home/liulu/app/hadoop/etc/hadoop/mapred-site.xml"));
//			String conf = "/home/liulu/app/hadoop/etc/hadoop/mapred-site.xml";
			JobClient jobClient = new JobClient(new JobConf(conf));
			JobStatus[] jobs = jobClient.getAllJobs();
			System.out.println("num : " + jobs.length);
			
			for(JobStatus job : jobs){
				System.out.println(job.getJobId());
				System.out.println(job.getRunState());
				System.out.println(job.getUsername());
				RunningJob rJob = jobClient.getJob(job.getJobID());
				System.out.println("get RunningJob");
				System.out.println("job conf file : " + rJob.getJobFile());
				org.apache.hadoop.mapred.Counters counters = rJob.getCounters();
				if("job_1408957289317_0012".equals(job.getJobID().toString())){
					System.out.println(counters.countCounters());
				}
				if("job_1408957289317_0005".equals(job.getJobID().toString())){
					System.out.println(counters.countCounters());
				}
				
//				printCounter(counters);
				
				System.out.println("============================");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	private static void testJobClient(String jobId){
		try {
			System.out.println("get job from history by JobClient...");
			String conf = "/home/liulu/app/hadoop/etc/hadoop/mapred-site.xml";
			JobClient jobClient = new JobClient(new JobConf(conf));
			RunningJob rJob = jobClient.getJob(jobId);
			System.out.println(rJob.getJobID());
			System.out.println(rJob.getJobState());
			System.out.println(rJob.getJobName());

			org.apache.hadoop.mapred.Counters counters = rJob.getCounters();
			if(counters == null){
				System.out.println("counters is null");
			}else{
				System.out.println("counters num : " + counters.countCounters());
			}
//			printCounter(counters);
			
			System.out.println("============================");
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

//		try {
//			testJobhistory();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println("***********************************");
		testJobClient();
		
		System.out.println("***********************************");
//		testJobClient("job_1408614584777_0006");
		
//		testFileStatus(args[0]);
		
//		testParseXML();
		
	}

}

