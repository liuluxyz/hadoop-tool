package analysejob.rule.job;

import java.util.Map;

import common.TimeFormater;

import analysejob.adapter.MRAdapter;
import analysejob.common.AnalyseResult;
import analysejob.common.job.JobConfKey;
import analysejob.common.job.MRBean;
import analysejob.common.job.counter.TaskCounterKey;


/**
 * liulu5
 * 2013-11-22
 */
public abstract class JobAnalyseRule {

	protected MRAdapter mrAdapter;
	
	public abstract AnalyseResult doAnalyse(MRBean mrBean) throws Exception;
	
	public abstract String[] getDesc();
	
	public abstract String[] getReason();
	
	public abstract String[] getSuggestion();
	
	/**
	 * 解析job的conf信息
	 * @param mrBean
	 * @param referInfo
	 */
	public void parseCommonJobInfo(MRBean mrBean, final Map<Object, Object> referInfo){
		String[] keys = new String[]{
				JobConfKey.dfsBlockSize,
				JobConfKey.tclName,
				JobConfKey.hiveStageName
		};
		for(String key : keys){
			String value = mrBean.getConf(key);
			if(value != null){
				referInfo.put(key, value);
			}
		}
		
		long takeTime = (mrBean.getJobEndTime() - mrBean.getJobStartTime()) / 1000;
		referInfo.put("job执行时长(s)", takeTime);
		if(mrBean.getTaskCounter().get(TaskCounterKey.CPU_MILLISECONDS.name()) != null){
			referInfo.put("job占用CPU时长(s)", TimeFormater.format(mrBean.getTaskCounter().get(TaskCounterKey.CPU_MILLISECONDS.name())));
		}
	}
}

