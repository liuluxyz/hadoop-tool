package analysejob.common.job;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.TIPStatus;

/**
 * 
 * @author yangjp
 * 
 */
public class TaskReportUDF {
	private String taskid;
	
	private TIPStatus status;
	/**
	 * 开始时间
	 */
	private long startTime;
	/**
	 * 结束时间
	 */
	private long finishTime;

	private Map<String, Long> fsCounter = new HashMap<String, Long>();

	private Map<String, Long> taskCounter = new HashMap<String, Long>();

	public String getTaskid() {
		return taskid;
	}

	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}

	public Map<String, Long> getFsCounter() {
		return fsCounter;
	}

	public void setFsCounter(Map<String, Long> fsCounter) {
		this.fsCounter = fsCounter;
	}

	public Map<String, Long> getTaskCounter() {
		return taskCounter;
	}

	public void setTaskCounter(Map<String, Long> taskCounter) {
		this.taskCounter = taskCounter;
	}

	public TIPStatus getStatus() {
		return status;
	}

	public void setStatus(TIPStatus status) {
		this.status = status;
	}

}
