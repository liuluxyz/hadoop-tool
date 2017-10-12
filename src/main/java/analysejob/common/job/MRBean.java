package analysejob.common.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MRBean {
	/**
	 * 文件系统计数器的存储
	 */
	private Map<String,Long> fsCounter = new HashMap<String,Long>();
	/**
	 * FileInputFormatCounter 文件输入格式计数器的存储
	 */
	private Map<String,Long> fifCounter = new HashMap<String,Long>();
	/**
	 * Job 的计数器存储
	 */
	private Map<String,Long> jobCounter = new HashMap<String,Long>();
	/**
	 * Task 任务计数器的存储
	 */
	private Map<String,Long> taskCounter = new HashMap<String,Long>();
	
	/**
	 * 每个job的map集合
	 * key : taskID, value: map --TaskReportUDF
	 * 
	 */
	private List<TaskReportUDF> maps = new ArrayList<TaskReportUDF>();
	/**
	 * 每个job的reduces集合
	 * key: taskID  value: reduce --TaskReportUDF
	 */
	private List<TaskReportUDF> reduces = new ArrayList<TaskReportUDF>();
	
	/**
	 * job的conf信息
	 */
	private Map<String,String> conf;
	
	/**
	 * 
	 */
	private String jobId;
	/**
	 * job名称
	 */
	private String jobName;
	
	/**
	 * 作业（job）开始时间
	 */
	private long jobStartTime;
	/**
	 * 作业（job）结束时间
	 */
	private long jobEndTime;
	
	/**
	 * map的数量
	 */
	private int mapNums;
	
	/**
	 * reduces数量
	 */
	private int reduceNums;
	
	/**
	 * job运行状态  1： 运行 -running 2：成功success 3:失败 failed 4：预备状态prep  5：杀死killed 
	 */
	private int jobRunStates;
	
	
	

	public String getJobId() {
		return jobId;
	}
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public long getJobStartTime() {
		return jobStartTime;
	}
	public void setJobStartTime(long jobStartTime) {
		this.jobStartTime = jobStartTime;
	}
	public long getJobEndTime() {
		return jobEndTime;
	}
	public void setJobEndTime(long jobEndTime) {
		this.jobEndTime = jobEndTime;
	}
	public int getMapNums() {
		return mapNums;
	}
	public void setMapNums(int mapNums) {
		this.mapNums = mapNums;
	}
	public int getReduceNums() {
		return reduceNums;
	}
	public void setReduceNums(int reduceNums) {
		this.reduceNums = reduceNums;
	}

	public Map<String, Long> getFsCounter() {
		return fsCounter;
	}
	public Map<String, Long> getFifCounter() {
		return fifCounter;
	}
	public Map<String, Long> getJobCounter() {
		return jobCounter;
	}
	public Map<String, Long> getTaskCounter() {
		return taskCounter;
	}
	public List<TaskReportUDF> getMaps() {
		return maps;
	}
	public List<TaskReportUDF> getReduces() {
		return reduces;
	}
	public int getJobRunStates() {
		return jobRunStates;
	}
	public void setJobRunStates(int jobRunStates) {
		this.jobRunStates = jobRunStates;
	}
	public void setConf(Map<String, String> conf) {
		this.conf = conf;
	}
	public Map<String, String> getConf() {
		return this.conf;
	}
	public String getConf(String key) {
		if(this.conf != null){
			return this.conf.get(key);
		}
		return null;
	}
	
}
