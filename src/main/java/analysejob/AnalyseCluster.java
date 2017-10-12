package analysejob;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobStatus;

import common.FilesizeFormater;
import common.NumberFormater;
import common.TimeFormater;
import common.WriteFileHelper;

import analysejob.adapter.MRAdapter;
import analysejob.common.ClusterStatisticResult;
import analysejob.common.job.JobConfKey;
import analysejob.common.job.MRBean;
import analysejob.common.job.TaskReportUDF;
import analysejob.common.job.counter.FSCounterKey;
import analysejob.common.job.counter.TaskCounterKey;

/**
 * liulu5
 * 2014-03-06
 */
public class AnalyseCluster {

	private static final Log log = LogFactory.getLog(AnalyseCluster.class);
	
	private int cycle = 1;//指定分析线程的间隔时间(单位为分钟)，不间断的进行分析,0表示只分析一次
	
	private Calendar lastAnalyseTime;//记录上次分析的时间
	
	private final String directory = "../report";
	
	LinkedHashMap<JobKind, ClusterStatisticResult> jobStats;
	
	private MRAdapter mrAdapter;
	
//	private enum JobKind{smallJob, bigJob, etlJob, tclJob, noGroupJob;
	private enum JobKind{smallJob, bigJob;
		public String getDisplayName(){
			if(this.ordinal() == smallJob.ordinal())
				return "小任务";
			if(this.ordinal() == bigJob.ordinal())
				return "大任务";
//			if(this.ordinal() == etlJob.ordinal())
//				return "ETL任务";
//			if(this.ordinal() == tclJob.ordinal())
//				return "TCL任务";
//			if(this.ordinal() == noGroupJob.ordinal())
//				return "未分组任务";
			return "";
		}
	};
	
	public boolean init(String[] args){
		log.info("init...");
		boolean result = initOption(args);
		if(result == false){
			return false;
		}

		reinitClusterResult();
		
		result = initAdapter();
		
		log.info("init result : " + result);
		return result;
	}
	
	public boolean initAdapter(){
		log.info("initAdapter...");
		try {
			mrAdapter = new MRAdapter();
		} catch (IOException e) {
			log.error("error: init MR adapter error [" + e.toString() + "]", e);
			return false;
		} catch (Exception e) {
			log.error("error: init MR adapter error [" + e.toString() + "]", e);
			return false;
		}
		return true;
	}
	
	public boolean initOption(String[] args){
		log.info("initOption...");
		int num = 0;
		while(num < args.length){
			try{
				if("-h".equals(args[num])){
					this.printUsage();
					return false;
				}
				else if("-cycle".equals(args[num])){
					cycle = Integer.parseInt(args[num + 1]);
					num += 2;
					continue;
				}
				else{
					log.info("error: not support parameter : " + args[num]);
					return false;
				}
			}catch(NumberFormatException e){
				log.error("error: " + args[num] + " should be number!", e);
				return false;
			}
		}
		return true;
	}
	
	public void printUsage(){
		log.info("用法: ./analyseCluster.sh [-选项] [参数]");
		log.info("	e.g. ./analyseCluster.sh");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-cycle	" 		+ "	指定分析线程的间隔时间(单位为分钟)，不间断的进行分析,0表示只分析一次");
	}
	
	private boolean isNewDay(){
		if(lastAnalyseTime == null){
			lastAnalyseTime = Calendar.getInstance();
			return false;
		}
		Calendar calendar = Calendar.getInstance();
		if(calendar.get(Calendar.DAY_OF_MONTH) != lastAnalyseTime.get(Calendar.DAY_OF_MONTH)){
			lastAnalyseTime = calendar;
			log.info("is new day : " + calendar.toString());
			return true;
		}
		return false;
	}
	
	private void reinitClusterResult(){
		log.info("reinitClusterResult...");
		if(jobStats == null){
			jobStats = new LinkedHashMap<JobKind, ClusterStatisticResult>();	
		}else{
			jobStats.clear();	
		}
		jobStats.put(JobKind.smallJob, new ClusterStatisticResult());
		jobStats.put(JobKind.bigJob, new ClusterStatisticResult());
//		jobStats.put(JobKind.etlJob, new ClusterStatisticResult());
//		jobStats.put(JobKind.tclJob, new ClusterStatisticResult());
//		jobStats.put(JobKind.noGroupJob, new ClusterStatisticResult());
	}
	
	public void start(){
		log.info("begin to analyse...");
		try {
			if(isNewDay()){
				reinitClusterResult();
			}
			
			List<MRBean> mrBeans = getAnalyseJobs();
			log.info("get analyse job num : " + mrBeans.size());
			if(mrBeans.size() == 0){
				return;
			}
			
			for(MRBean mrBean : mrBeans){
				if(mrBean.getMapNums() + mrBean.getReduceNums() < 10){
					updateStatisticResult(jobStats.get(JobKind.smallJob), mrBean);
				}
				else{
					updateStatisticResult(jobStats.get(JobKind.bigJob), mrBean);
				}
				
//				if(mrBean.getConf(JobConfKey.tclName) != null){
//					updateStatisticResult(jobStats.get(JobKind.tclJob), mrBean);
//				}
//				else if(isETLJob(mrBean.getConf(JobConfKey.jobName))){
//					updateStatisticResult(jobStats.get(JobKind.etlJob), mrBean);
//				}
//				else{
//					updateStatisticResult(jobStats.get(JobKind.noGroupJob), mrBean);
//				}
			}

			String report = parseReport();
			
			writeReport(report);
			
		} catch (Exception e) {
			log.error("", e);
		}
		
		log.info("end to analyse...");
	}
	
	private void updateStatisticResult(final ClusterStatisticResult statResult, final MRBean mrBean){
		log.info("begin updateStatisticResult..." + mrBean.getJobId());
		if(statResult.containJobid(mrBean.getJobId())){
			log.info("duplicate job : " + mrBean.getJobId());
			return;
		}
		
		statResult.addJobid(mrBean.getJobId());
		statResult.setEnd(new Date());
		
		statResult.addMapNum(mrBean.getMapNums());
		statResult.addReduceNum(mrBean.getReduceNums());
	
//		log.info("addJobSpendTime : " + mrBean.getJobId() + " : " + (mrBean.getJobEndTime() - mrBean.getJobStartTime()));
//		log.info("jobSpendTime : " + statResult.getJobSpendTime() + " : " + TimeFormater.format(statResult.getJobSpendTime()));
		statResult.addJobSpendTime(mrBean.getJobEndTime() - mrBean.getJobStartTime());
		statResult.addJobSpendCPUTime(mrBean.getTaskCounter().get(TaskCounterKey.CPU_MILLISECONDS.name()));
		
		for(TaskReportUDF task : mrBean.getMaps()){
			statResult.addMapSpendTime(task.getFinishTime() - task.getStartTime());
			statResult.addMapSpendCPUTime(task.getTaskCounter().get(TaskCounterKey.CPU_MILLISECONDS.name()));
			
			statResult.addMapFileReadBytes(task.getFsCounter().get(FSCounterKey.FILE_BYTES_READ.name()));
			statResult.addMapFileWrittenBytes(task.getFsCounter().get(FSCounterKey.FILE_BYTES_WRITTEN.name()));
			statResult.addMapHDFSReadBytes(task.getFsCounter().get(FSCounterKey.HDFS_BYTES_READ.name()));
			statResult.addMapHDFSWrittenBytes(task.getFsCounter().get(FSCounterKey.HDFS_BYTES_WRITTEN.name()));
		}
		for(TaskReportUDF task : mrBean.getReduces()){
			statResult.addReduceSpendTime(task.getFinishTime() - task.getStartTime());
			statResult.addReduceSpendCPUTime(task.getTaskCounter().get(TaskCounterKey.CPU_MILLISECONDS.name()));
			
			statResult.addReduceFileReadBytes(task.getFsCounter().get(FSCounterKey.FILE_BYTES_READ.name()));
			statResult.addReduceFileWrittenBytes(task.getFsCounter().get(FSCounterKey.FILE_BYTES_WRITTEN.name()));
			statResult.addReduceHDFSReadBytes(task.getFsCounter().get(FSCounterKey.HDFS_BYTES_READ.name()));
			statResult.addReduceHDFSWrittenBytes(task.getFsCounter().get(FSCounterKey.HDFS_BYTES_WRITTEN.name()));
		}
		
		statResult.addMapInputRecords(mrBean.getTaskCounter().get(TaskCounterKey.MAP_INPUT_RECORDS.name()));
		statResult.addMapOutputRecords(mrBean.getTaskCounter().get(TaskCounterKey.MAP_OUTPUT_RECORDS.name()));
		if(mrBean.getReduceNums() > 0){
			statResult.addReduceInputRecords(mrBean.getTaskCounter().get(TaskCounterKey.REDUCE_INPUT_RECORDS.name()));
			statResult.addReduceOutputRecords(mrBean.getTaskCounter().get(TaskCounterKey.REDUCE_OUTPUT_RECORDS.name()));	
		}
		
		statResult.addFileReadBytes(mrBean.getFsCounter().get(FSCounterKey.FILE_BYTES_READ.name()));
		statResult.addFileWrittenBytes(mrBean.getFsCounter().get(FSCounterKey.FILE_BYTES_WRITTEN.name()));
		statResult.addHdfsReadBytes(mrBean.getFsCounter().get(FSCounterKey.HDFS_BYTES_READ.name()));
		statResult.addHdfsWrittenBytes(mrBean.getFsCounter().get(FSCounterKey.HDFS_BYTES_WRITTEN.name()));
		
		log.info("end updateStatisticResult..." + mrBean.getJobId());
	}
	
	private boolean isETLJob(String jobName){
		if(jobName == null || "".equals(jobName)){
			return false;
		}
		return ("FTP_UDF".equals(jobName)) || (jobName.split("-").length >= 4) || (jobName.contains("ReadFileSize")) || (jobName.contains("distcp"));
	}
	
	private String parseReport(){
		log.info("begin parseReport...");
		StringBuffer report = new StringBuffer();
		
		report.append("job总数 : " + (jobStats.get(JobKind.smallJob).getJobNum() + jobStats.get(JobKind.bigJob).getJobNum())).append("\n");
		
		Iterator<JobKind> it = jobStats.keySet().iterator();
		while(it.hasNext()){
			JobKind kind = it.next();
			report.append("	" + kind.getDisplayName() + "数量 : " + jobStats.get(kind).getJobNum()).append("\n");
		}
		
		it = jobStats.keySet().iterator();
		while(it.hasNext()){
			JobKind kind = it.next();
			ClusterStatisticResult res = jobStats.get(kind);
			report.append("------------------------------------------\n");
			report.append(kind.getDisplayName() + "统计 : ").append("\n");
			report.append("	时间 : " + res.getStart().toLocaleString() + " --> " + res.getEnd().toLocaleString()).append("\n");
			
			report.append("	map数 : " + res.getMapNum()).append("\n");
			report.append("	reduce数 : " + res.getReduceNum()).append("\n");
			
			report.append("	job执行总时长 : " + TimeFormater.format(res.getJobSpendTime())).append("\n");
			report.append("	map执行总时长 : " + TimeFormater.format(res.getMapSpendTime())).append("\n");
			report.append("	reduce执行总时长 : " + TimeFormater.format(res.getReduceSpendTime())).append("\n");
			report.append("	job执行总CPU时长 : " + TimeFormater.format(res.getJobSpendCPUTime())).append("\n");
			report.append("	map执行总CPU时长 : " + TimeFormater.format(res.getMapSpendCPUTime())).append("\n");
			report.append("	reduce执行总CPU时长 : " + TimeFormater.format(res.getReduceSpendCPUTime())).append("\n");
			
			report.append("	map输入记录数 : " + NumberFormater.format(res.getMapInputRecords())).append("\n");
			report.append("	map输出记录数 : " + NumberFormater.format(res.getMapOutputRecords())).append("\n");
			report.append("	reduce输入记录数 : " + NumberFormater.format(res.getReduceInputRecords())).append("\n");
			report.append("	reduce输出记录数 : " + NumberFormater.format(res.getReduceOutputRecords())).append("\n");
			
			report.append("	map读取本地文件数据总大小 : " + FilesizeFormater.format(res.getMapFileReadBytes())).append("\n");
			report.append("	map写入本地文件数据总大小 : " + FilesizeFormater.format(res.getMapFileWrittenBytes())).append("\n");
			report.append("	map读取HDFS文件数据总大小 : " + FilesizeFormater.format(res.getMapHDFSReadBytes())).append("\n");
			report.append("	map写入HDFS文件数据总大小 : " + FilesizeFormater.format(res.getMapHDFSWrittenBytes())).append("\n");
			report.append("	map读取数据总大小 : " + FilesizeFormater.format((res.getMapFileReadBytes() + res.getMapHDFSReadBytes()))).append("\n");
			
			report.append("	reduce读取本地文件数据总大小 : " + FilesizeFormater.format(res.getReduceFileReadBytes())).append("\n");
			report.append("	reduce写入本地文件数据总大小 : " + FilesizeFormater.format(res.getReduceFileWrittenBytes())).append("\n");
			report.append("	reduce读取HDFS文件数据总大小 : " + FilesizeFormater.format(res.getReduceHDFSReadBytes())).append("\n");
			report.append("	reduce写入HDFS文件数据总大小 : " + FilesizeFormater.format(res.getReduceHDFSWrittenBytes())).append("\n");
			
			report.append("	读取本地文件数据总大小 : " + FilesizeFormater.format(res.getFileReadBytes())).append("\n");
			report.append("	写入本地文件数据总大小 : " + FilesizeFormater.format(res.getFileWrittenBytes())).append("\n");
			report.append("	读取HDFS文件数据总大小 : " + FilesizeFormater.format(res.getHdfsReadBytes())).append("\n");
			report.append("	写入HDFS文件数据总大小 : " + FilesizeFormater.format(res.getHdfsWrittenBytes())).append("\n");
			
			report.append("	job执行平均时长 : " + TimeFormater.format(res.getJobSpendTime() / res.getJobNum())).append("\n");
			report.append("	map执行平均时长 : " + TimeFormater.format(res.getMapSpendTime() / res.getMapNum())).append("\n");
			if(res.getReduceNum() == 0){
				report.append("	reduce执行平均时长 : " + 0).append("\n");
			}else{
				report.append("	reduce执行平均时长 : " + TimeFormater.format(res.getReduceSpendTime() / res.getReduceNum())).append("\n");	
			}
			report.append("	job执行平均CPU时长 : " + TimeFormater.format(res.getJobSpendCPUTime() / res.getJobNum())).append("\n");
			report.append("	map执行平均CPU时长 : " + TimeFormater.format(res.getMapSpendCPUTime() / res.getMapNum())).append("\n");
			if(res.getReduceNum() == 0){
				report.append("	reduce执行平均CPU时长 : " + 0).append("\n");	
			}else{
				report.append("	reduce执行平均CPU时长 : " + TimeFormater.format(res.getReduceSpendCPUTime() / res.getReduceNum())).append("\n");
			}
			
			report.append("	map平均处理数据量 : " + FilesizeFormater.format(((res.getMapFileReadBytes() + res.getMapHDFSReadBytes()) / (double)res.getMapNum()) * 100 / 100.0)).append("\n");
			if(res.getReduceNum() == 0){
				report.append("	reduce写入HDFS的平均数据量 : " + 0).append("\n");	
			}else{
				report.append("	reduce写入HDFS的平均数据量 : " + FilesizeFormater.format((res.getReduceHDFSWrittenBytes() / (double)res.getReduceNum()) * 100 / 100.0)).append("\n");
			}
		}
		
		log.info("end parseReport...");
		return report.toString();
	}
	
	
	/**
	 * 获取需要分析的job
	 * @return
	 * @throws IOException
	 */
	private List<MRBean> getAnalyseJobs() throws IOException{
		log.info("begin getAnalyseJobs...");
		List<MRBean> mrBeans = new ArrayList<MRBean>();
		
		List<MRBean> allJobs = mrAdapter.getMRInfo();
		log.info("从jobclient获取了[" + allJobs.size() + "]个job");
		int noAnalyseNum = 0;
		for(MRBean job : allJobs){//根据过滤条件过滤需要分析的job
			if(job.getConf().size() == 0){
				log.info("无法获取job conf,不分析 : " + job.getJobId());
				noAnalyseNum++;
				continue;
			}
			if(job.getJobRunStates() != JobStatus.SUCCEEDED){
				log.info("job未完成，不分析: " + job.getJobId());
				noAnalyseNum++;
				continue;
			}
			mrBeans.add(job);
		}
		log.info("从[" + allJobs.size() + "]个job中过滤掉了[" + noAnalyseNum + "]个job，需要分析的job个数为:["+ mrBeans.size() + "]");
		log.info("end getAnalyseJobs...");
		return mrBeans;
	}
	
	public int getCycle() {
		return cycle;
	}

	private void writeReport(String report){
		log.info("begin writeReport...");
		String filepath = directory + "/cluster_" + SimpleDateFormat.getDateInstance().format(lastAnalyseTime.getTime()) + ".report";
		log.info("write report into file : " + filepath);
		WriteFileHelper writer = new WriteFileHelper(true, filepath);
		writer.writeOverwrite(report);
		log.info("end writeReport...");
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		AnalyseCluster analyse = new AnalyseCluster();
		boolean result = analyse.init(args);
		if(result == false){
			return;
		}
		
		log.info("开始循环分析, 间隔时间(m)为 ：" + analyse.getCycle());
		int num = 1;
		while(true){
			log.info("开始分析第" + num + "次");
			analyse.start();
			log.info("结束分析第" + num + "次");
			
			try {
				log.info("start sleep("+(analyse.getCycle() * 60)+"s)...");
				Thread.sleep(analyse.getCycle() * 60 * 1000);
				log.info("end sleep...");
			} catch (InterruptedException e) {
				log.error("", e);
			}
			num++;
		}
		
	}
}