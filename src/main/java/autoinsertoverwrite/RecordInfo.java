package autoinsertoverwrite;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * liulu5
 * 2013-12-16
 */
public class RecordInfo {

	//para info:
	private boolean isInsert;
	private String sourceDB;
	private String targetDB;
	
	//progress info:
	private int doInsertTotalNum;
	private int successNum;
	private int failNum;
	
	//statistics info:
	Map<String, Object> statisticsInfo;
	List<String> failMessageList;
	
	public RecordInfo(){
		isInsert = false;
		sourceDB = null;
		targetDB = null;
		
		doInsertTotalNum = 0;
		successNum = 0;
		failNum = 0;
		
		statisticsInfo = new LinkedHashMap<String, Object>();
		failMessageList = new ArrayList<String>();
	}
	
	public boolean isInsert() {
		return isInsert;
	}
	public void setInsert(boolean isInsert) {
		this.isInsert = isInsert;
	}
	public int getDoInsertTotalNum() {
		return doInsertTotalNum;
	}
	public void setDoInsertTotalNum(int doInsertTotalNum) {
		this.doInsertTotalNum = doInsertTotalNum;
	}
	public int getSuccessNum() {
		return successNum;
	}
	public int getFailNum() {
		return failNum;
	}
	public int addSuccessNum() {
		return successNum++;
	}
	public int addFailNum() {
		return failNum++;
	}
	public String getSourceDB() {
		return sourceDB;
	}

	public void setSourceDB(String sourceDB) {
		this.sourceDB = sourceDB;
	}

	public String getTargetDB() {
		return targetDB;
	}

	public void setTargetDB(String targetDB) {
		this.targetDB = targetDB;
	}

	public void addStatisticsInfo(String key, Object value){
		statisticsInfo.put(key, value);
	}
	
	public void addFailMessage(String message){
		failMessageList.add(message);
	}
	
	public Map<String, Object> getStatisticsInfo() {
		return statisticsInfo;
	}

	public List<String> getFailMessageList() {
		return failMessageList;
	}

	public String getParameterInfo(){
		return "[isInsert : " + isInsert + "] [sourceDB : " + sourceDB + "] [targetDB : " + targetDB + "]";
	}
	
	public String getProgressInfo(){
		return "[success num : " + successNum + "] [fail num : " + failNum + "] [do insert total num : " + doInsertTotalNum + "]";
	}
}

