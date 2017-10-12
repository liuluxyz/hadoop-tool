package analysejob.common;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.FilesizeFormater;
import common.NumberFormater;
import common.TimeFormater;

/**
 * liulu5
 * 2014-3-6
 */
public class ClusterStatisticResult {

	private static final Log log = LogFactory.getLog(ClusterStatisticResult.class);
	
	private Date start;
	private Date end;
	
	private List<String> jobidList;
	
	private int mapNum;
	private int reduceNum;
	
	private long jobSpendTime;
	private long mapSpendTime;
	private long reduceSpendTime;
	
	private long jobSpendCPUTime;
	private long mapSpendCPUTime;
	private long reduceSpendCPUTime;
	
	private long mapInputRecords;
	private long mapOutputRecords;
	private long reduceInputRecords;
	private long reduceOutputRecords;
	
	private long mapFileReadBytes;
	private long mapFileWrittenBytes;
	private long mapHDFSReadBytes;
	private long mapHDFSWrittenBytes;
	
	private long reduceFileReadBytes;
	private long reduceFileWrittenBytes;
	private long reduceHDFSReadBytes;
	private long reduceHDFSWrittenBytes;
	
	private long fileReadBytes;
	private long fileWrittenBytes;
	private long hdfsReadBytes;
	private long hdfsWrittenBytes;
	
	public ClusterStatisticResult(){
		start = new Date();
		jobidList = new ArrayList<String>();
		mapNum = 0;
		reduceNum = 0;
		jobSpendTime = 0;
		mapSpendTime = 0;
		reduceSpendTime = 0;
		jobSpendCPUTime = 0;
		mapSpendCPUTime = 0;
		reduceSpendCPUTime = 0;
		mapInputRecords = 0;
		mapOutputRecords = 0;
		reduceInputRecords = 0;
		reduceOutputRecords = 0;
		fileReadBytes = 0;
		fileWrittenBytes = 0;
		hdfsReadBytes = 0;
		hdfsWrittenBytes = 0;
	}

	public Date getStart() {
		return start;
	}

//	public void setStart(Date start) {
//		this.start = start;
//	}

	public Date getEnd() {
		return end;
	}

	public void setEnd(Date end) {
		this.end = end;
	}

//	public List<String> getJobidList() {
//		return jobidList;
//	}

	public int getJobNum() {
		return jobidList.size();
	}

	public boolean containJobid(String jobid) {
		return jobidList.contains(jobid);
	}
	
	public void addJobid(String jobid) {
		this.jobidList.add(jobid);
	}

	public int getMapNum() {
		return mapNum;
	}

	public void addMapNum(int mapNum) {
		log.info("mapNum : " + NumberFormater.format(this.mapNum));
		log.info("addMapNum : " + NumberFormater.format(mapNum));
		this.mapNum += mapNum;
		log.info("mapNum : " + NumberFormater.format(this.mapNum));
	}

	public int getReduceNum() {
		return reduceNum;
	}

	public void addReduceNum(int reduceNum) {
		log.info("reduceNum : " + NumberFormater.format(this.reduceNum));
		log.info("addReduceNum : " + NumberFormater.format(reduceNum));
		this.reduceNum += reduceNum;
		log.info("reduceNum : " + NumberFormater.format(this.reduceNum));
	}

	public long getJobSpendTime() {
		return jobSpendTime;
	}

	public void addJobSpendTime(long jobSpendTime) {
		log.info("jobSpendTime : " + TimeFormater.format(this.jobSpendTime));
		log.info("addJobSpendTime : " + TimeFormater.format(jobSpendTime));
		this.jobSpendTime += jobSpendTime;
		log.info("jobSpendTime : " + TimeFormater.format(this.jobSpendTime));
	}

	public long getMapSpendTime() {
		return mapSpendTime;
	}

	public void addMapSpendTime(long mapSpendTime) {
		log.info("mapSpendTime : " + TimeFormater.format(this.mapSpendTime));
		log.info("addMapSpendTime : " + TimeFormater.format(mapSpendTime));
		this.mapSpendTime += mapSpendTime;
		log.info("mapSpendTime : " + TimeFormater.format(this.mapSpendTime));
	}

	public long getReduceSpendTime() {
		return reduceSpendTime;
	}

	public void addReduceSpendTime(long reduceSpendTime) {
		log.info("reduceSpendTime : " + TimeFormater.format(this.reduceSpendTime));
		log.info("addReduceSpendTime : " + TimeFormater.format(reduceSpendTime));
		this.reduceSpendTime += reduceSpendTime;
		log.info("reduceSpendTime : " + TimeFormater.format(this.reduceSpendTime));
	}

	public long getJobSpendCPUTime() {
		return jobSpendCPUTime;
	}

	public void addJobSpendCPUTime(long jobSpendCPUTime) {
		log.info("jobSpendCPUTime : " + TimeFormater.format(this.jobSpendCPUTime));
		log.info("addJobSpendCPUTime : " + TimeFormater.format(jobSpendCPUTime));
		this.jobSpendCPUTime += jobSpendCPUTime;
		log.info("jobSpendCPUTime : " + TimeFormater.format(this.jobSpendCPUTime));
	}

	public long getMapSpendCPUTime() {
		return mapSpendCPUTime;
	}

	public void addMapSpendCPUTime(long mapSpendCPUTime) {
		log.info("mapSpendCPUTime : " + TimeFormater.format(this.mapSpendCPUTime));
		log.info("addMapSpendCPUTime : " + TimeFormater.format(mapSpendCPUTime));
		this.mapSpendCPUTime += mapSpendCPUTime;
		log.info("mapSpendCPUTime : " + TimeFormater.format(this.mapSpendCPUTime));
	}

	public long getReduceSpendCPUTime() {
		return reduceSpendCPUTime;
	}

	public void addReduceSpendCPUTime(long reduceSpendCPUTime) {
		log.info("reduceSpendCPUTime : " + TimeFormater.format(this.reduceSpendCPUTime));
		log.info("addReduceSpendCPUTime : " + TimeFormater.format(reduceSpendCPUTime));
		this.reduceSpendCPUTime += reduceSpendCPUTime;
		log.info("reduceSpendCPUTime : " + TimeFormater.format(this.reduceSpendCPUTime));
	}

	public long getMapInputRecords() {
		return mapInputRecords;
	}

	public void addMapInputRecords(long mapInputRecords) {
		log.info("mapInputRecords : " + NumberFormater.format(this.mapInputRecords));
		log.info("addMapInputRecords : " + NumberFormater.format(mapInputRecords));
		this.mapInputRecords += mapInputRecords;
		log.info("mapInputRecords : " + NumberFormater.format(this.mapInputRecords));
	}

	public long getMapOutputRecords() {
		return mapOutputRecords;
	}

	public void addMapOutputRecords(long mapOutputRecords) {
		log.info("mapOutputRecords : " + NumberFormater.format(this.mapOutputRecords));
		log.info("addMapOutputRecords : " + NumberFormater.format(mapOutputRecords));
		this.mapOutputRecords += mapOutputRecords;
		log.info("mapOutputRecords : " + NumberFormater.format(this.mapOutputRecords));
	}

	public long getReduceInputRecords() {
		return reduceInputRecords;
	}

	public void addReduceInputRecords(long reduceInputRecords) {
		log.info("reduceInputRecords : " + NumberFormater.format(this.reduceInputRecords));
		log.info("addReduceInputRecords : " + NumberFormater.format(reduceInputRecords));
		this.reduceInputRecords += reduceInputRecords;
		log.info("reduceInputRecords : " + NumberFormater.format(this.reduceInputRecords));
	}

	public long getReduceOutputRecords() {
		return reduceOutputRecords;
	}

	public void addReduceOutputRecords(long reduceOutputRecords) {
		log.info("reduceOutputRecords : " + NumberFormater.format(this.reduceOutputRecords));
		log.info("addReduceOutputRecords : " + NumberFormater.format(reduceOutputRecords));
		this.reduceOutputRecords += reduceOutputRecords;
		log.info("reduceOutputRecords : " + NumberFormater.format(this.reduceOutputRecords));
	}

	public long getMapFileReadBytes() {
		return mapFileReadBytes;
	}

	public void addMapFileReadBytes(long mapFileReadBytes) {
		log.info("mapFileReadBytes : " + FilesizeFormater.format(this.mapFileReadBytes));
		log.info("addMapFileReadBytes : " + FilesizeFormater.format(mapFileReadBytes));
		this.mapFileReadBytes += mapFileReadBytes;
		log.info("mapFileReadBytes : " + FilesizeFormater.format(this.mapFileReadBytes));
	}

	public long getMapFileWrittenBytes() {
		return mapFileWrittenBytes;
	}

	public void addMapFileWrittenBytes(long mapFileWrittenBytes) {
		log.info("mapFileWrittenBytes : " + FilesizeFormater.format(this.mapFileWrittenBytes));
		log.info("addMapFileWrittenBytes : " + FilesizeFormater.format(mapFileWrittenBytes));
		this.mapFileWrittenBytes += mapFileWrittenBytes;
		log.info("mapFileWrittenBytes : " + FilesizeFormater.format(this.mapFileWrittenBytes));
	}

	public long getMapHDFSReadBytes() {
		return mapHDFSReadBytes;
	}

	public void addMapHDFSReadBytes(long mapHDFSReadBytes) {
		log.info("mapHDFSReadBytes : " + FilesizeFormater.format(this.mapHDFSReadBytes));
		log.info("addMapHDFSReadBytes : " + FilesizeFormater.format(mapHDFSReadBytes));
		this.mapHDFSReadBytes += mapHDFSReadBytes;
		log.info("mapHDFSReadBytes : " + FilesizeFormater.format(this.mapHDFSReadBytes));
	}

	public long getMapHDFSWrittenBytes() {
		return mapHDFSWrittenBytes;
	}

	public void addMapHDFSWrittenBytes(long mapHDFSWrittenBytes) {
		log.info("mapHDFSWrittenBytes : " + FilesizeFormater.format(this.mapHDFSWrittenBytes));
		log.info("addMapHDFSWrittenBytes : " + FilesizeFormater.format(mapHDFSWrittenBytes));
		this.mapHDFSWrittenBytes += mapHDFSWrittenBytes;
		log.info("mapHDFSWrittenBytes : " + FilesizeFormater.format(this.mapHDFSWrittenBytes));
	}

	public long getReduceFileReadBytes() {
		return reduceFileReadBytes;
	}

	public void addReduceFileReadBytes(long reduceFileReadBytes) {
		log.info("reduceFileReadBytes : " + FilesizeFormater.format(this.reduceFileReadBytes));
		log.info("addReduceFileReadBytes : " + FilesizeFormater.format(reduceFileReadBytes));
		this.reduceFileReadBytes += reduceFileReadBytes;
		log.info("reduceFileReadBytes : " + FilesizeFormater.format(this.reduceFileReadBytes));
	}

	public long getReduceFileWrittenBytes() {
		return reduceFileWrittenBytes;
	}

	public void addReduceFileWrittenBytes(long reduceFileWrittenBytes) {
		log.info("reduceFileWrittenBytes : " + FilesizeFormater.format(this.reduceFileWrittenBytes));
		log.info("addReduceFileWrittenBytes : " + FilesizeFormater.format(reduceFileWrittenBytes));
		this.reduceFileWrittenBytes += reduceFileWrittenBytes;
		log.info("reduceFileWrittenBytes : " + FilesizeFormater.format(this.reduceFileWrittenBytes));
	}

	public long getReduceHDFSReadBytes() {
		return reduceHDFSReadBytes;
	}

	public void addReduceHDFSReadBytes(long reduceHDFSReadBytes) {
		log.info("reduceHDFSReadBytes : " + FilesizeFormater.format(this.reduceHDFSReadBytes));
		log.info("addReduceHDFSReadBytes : " + FilesizeFormater.format(reduceHDFSReadBytes));
		this.reduceHDFSReadBytes += reduceHDFSReadBytes;
		log.info("reduceHDFSReadBytes : " + FilesizeFormater.format(this.reduceHDFSReadBytes));
	}

	public long getReduceHDFSWrittenBytes() {
		return reduceHDFSWrittenBytes;
	}

	public void addReduceHDFSWrittenBytes(long reduceHDFSWrittenBytes) {
		log.info("reduceHDFSWrittenBytes : " + FilesizeFormater.format(this.reduceHDFSWrittenBytes));
		log.info("addReduceHDFSWrittenBytes : " + FilesizeFormater.format(reduceHDFSWrittenBytes));
		this.reduceHDFSWrittenBytes += reduceHDFSWrittenBytes;
		log.info("reduceHDFSWrittenBytes : " + FilesizeFormater.format(this.reduceHDFSWrittenBytes));
	}

	public long getFileReadBytes() {
		return fileReadBytes;
	}

	public void addFileReadBytes(long fileReadBytes) {
		log.info("fileReadBytes : " + FilesizeFormater.format(this.fileReadBytes));
		log.info("addFileReadBytes : " + FilesizeFormater.format(fileReadBytes));
		this.fileReadBytes += fileReadBytes;
		log.info("fileReadBytes : " + FilesizeFormater.format(this.fileReadBytes));
	}

	public long getFileWrittenBytes() {
		return fileWrittenBytes;
	}

	public void addFileWrittenBytes(long fileWrittenBytes) {
		log.info("fileWrittenBytes : " + FilesizeFormater.format(this.fileWrittenBytes));
		log.info("addFileWrittenBytes : " + FilesizeFormater.format(fileWrittenBytes));
		this.fileWrittenBytes += fileWrittenBytes;
		log.info("fileWrittenBytes : " + FilesizeFormater.format(this.fileWrittenBytes));
	}

	public long getHdfsReadBytes() {
		return hdfsReadBytes;
	}

	public void addHdfsReadBytes(long hdfsReadBytes) {
		log.info("hdfsReadBytes : " + FilesizeFormater.format(this.hdfsReadBytes));
		log.info("addHdfsReadBytes : " + FilesizeFormater.format(hdfsReadBytes));
		this.hdfsReadBytes += hdfsReadBytes;
		log.info("hdfsReadBytes : " + FilesizeFormater.format(this.hdfsReadBytes));
	}

	public long getHdfsWrittenBytes() {
		return hdfsWrittenBytes;
	}

	public void addHdfsWrittenBytes(long hdfsWrittenBytes) {
		log.info("hdfsWrittenBytes : " + FilesizeFormater.format(this.hdfsWrittenBytes));
		log.info("addHdfsWrittenBytes : " + FilesizeFormater.format(hdfsWrittenBytes));
		this.hdfsWrittenBytes += hdfsWrittenBytes;
		log.info("hdfsWrittenBytes : " + FilesizeFormater.format(this.hdfsWrittenBytes));
	}
}

