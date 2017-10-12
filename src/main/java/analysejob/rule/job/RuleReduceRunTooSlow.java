package analysejob.rule.job;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import analysejob.adapter.MRAdapter;
import analysejob.common.AnalyseResult;
import analysejob.common.job.JobConfKey;
import analysejob.common.job.MRBean;
import analysejob.common.job.TaskReportUDF;

/**
 * liulu5
 * 2013-11-21
 */
public class RuleReduceRunTooSlow extends JobAnalyseRule{
	
	private static final Log log = LogFactory.getLog(RuleReduceRunTooSlow.class);
	
	private static final long runTimeThreshold = 5 * 60;//5min
	private double runTimeOffsetRate = 1.3;//运行时间与规定的时间阀值之间的偏移率,大于此值，则认为执行时间偏离规定的阀值
	
	public RuleReduceRunTooSlow(MRAdapter mrAdapter){
		this.mrAdapter = mrAdapter;
	}

	@Override
	public AnalyseResult doAnalyse(MRBean mrBean) throws Exception {
		log.info("analyse : " + mrBean.getJobId());
		
		List<TaskReportUDF> reduces = mrBean.getReduces();
		if(reduces.size() <= 0){
			return new AnalyseResult(toString(), AnalyseResult.Result.fail, new String[]{"no reduce"}, null, null);
		}
				
		Map<Object, Object> referInfo = new LinkedHashMap<Object, Object>();
		parseCommonJobInfo(mrBean, referInfo);
		referInfo.put("reduce总数", reduces.size());
		referInfo.put("reduce执行时长的最大阀值(s)", runTimeThreshold);
		referInfo.put("reduce执行时长与阀值之间的偏移率设定", runTimeOffsetRate);
		
		int tooLongTaskNum = 0;
		long[] timediffs = new long[reduces.size()];
		for(int i=0; i<reduces.size(); i++){
			timediffs[i] = (reduces.get(i).getFinishTime() - reduces.get(i).getStartTime()) / 1000;
			double rate = (double)timediffs[i] / (double) runTimeThreshold;
			if(rate > runTimeOffsetRate){//run time is too long
				tooLongTaskNum++;
			}
		}
		
		if(tooLongTaskNum > 0){
			Arrays.sort(timediffs);
			referInfo.put("执行过长的reduce个数", tooLongTaskNum);
			referInfo.put("所有reduce的执行时长(s)", Arrays.toString(timediffs));
			if(mrBean.getConf(JobConfKey.hiveQueryString) != null){
				referInfo.put("HQL", mrBean.getConf(JobConfKey.hiveQueryString));
			}
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, getDesc(), getReason(), getSuggestion(), referInfo);
		}
		
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null);
	}
	
	@Override
	public String toString() {
		return "检查reduce是否执行过慢";
	}

	@Override
	public String[] getDesc() {
		return new String[]{"一些reduce运行过慢"};
	}

	@Override
	public String[] getSuggestion() {
		return new String[]{"检查是否有数据倾斜"};
	}

	@Override
	public String[] getReason() {
		return new String[0];
	}
}

