package analysejob.rule.job;

import java.util.Arrays;
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
 * 检查map的执行时间是否可优化
 * liulu5
 * 2013-11-21
 */
public class RuleMapRunTooSlow extends JobAnalyseRule{

	private static final Log log = LogFactory.getLog(RuleMapRunTooSlow.class);
	
	private double runTimeOffsetRate = 1.3;//运行时间与规定的时间阀值之间的偏移率,大于此值，则认为执行时间偏离规定的阀值
	private double runTimeThreshold = 5 * 60;//5min
	private double offsetNumRate = 0.3;//偏离规定时间的task占总task的比率阀值，超过此值，则认为job的整体运行不正常
	private int offsetAverageRate = 2;//偏离平均时间的比率阀值，超过此值，则认为task超过平均值
	
	public RuleMapRunTooSlow(MRAdapter mrAdapter){
		this.mrAdapter = mrAdapter;
	}

	@Override
	public AnalyseResult doAnalyse(MRBean mrBean) throws Exception {
		log.info("analyse : " + mrBean.getJobId());
		
		List<TaskReportUDF> maps = mrBean.getMaps();
		
		Map<Object, Object> referInfo = new LinkedHashMap<Object, Object>();
		parseCommonJobInfo(mrBean, referInfo);
		referInfo.put("map总数", maps.size());
		referInfo.put("map执行时长阀值(s)", runTimeThreshold);
		referInfo.put("map执行时长与阀值之间的偏移率设定", runTimeOffsetRate);
		
		//rule: map执行时间过短或者过长。则需要优化
		int tooLongTask = 0;
		long timeTotal = 0;
		long[] timediffs = new long[maps.size()];
		for(int i=0; i<maps.size(); i++){
			timediffs[i] = (maps.get(i).getFinishTime() - maps.get(i).getStartTime()) / 1000;
			timeTotal += timediffs[i];
			double rate = (double)timediffs[i] / (double) runTimeThreshold;
			if(rate > runTimeOffsetRate){//run time is too long
				tooLongTask++;
			}
		}
		
		long spendTimeAverage = timeTotal / maps.size();
		referInfo.put("map执行时长平均值(s)", spendTimeAverage);
		int offsetAverageNum = 0;
		for(long spendTime : timediffs){
			if((spendTime / spendTimeAverage) > offsetAverageRate)
				offsetAverageNum++;
		}
		referInfo.put("超过平均时长" + offsetAverageRate + "倍的map数(s)", offsetAverageNum);
		
		double offsetRate = Math.round((double)tooLongTask / (double)maps.size() * 100) / 100.0;
		if(offsetRate > offsetNumRate){
			Arrays.sort(timediffs);
			referInfo.put("过慢的 map个数", tooLongTask);
			referInfo.put("过慢的map数占比", offsetRate*100 + "%");
			referInfo.put("每个map的执行时长(s)", Arrays.toString(timediffs));
			if(mrBean.getConf(JobConfKey.hiveQueryString) != null){
				referInfo.put("HQL", mrBean.getConf(JobConfKey.hiveQueryString));
			}
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, getDesc(), getReason(), getSuggestion(), referInfo);
		}
		
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null);
	}

	@Override
	public String toString() {
		return "检查map是否执行过慢";
	}

	@Override
	public String[] getDesc() {
		return new String[]{"超过" + (offsetNumRate*100) + "%的map运行过慢"};
	}

	@Override
	public String[] getSuggestion() {
		return new String[]{
				"将参数[mapred.max.split.size, mapred.min.split.size, mapred.min.split.size.per.node, mapred.min.split.size.per.rack]设置为更小值",
		};
	}

	@Override
	public String[] getReason() {
		return new String[]{
				"处理数据量过大",
				"处理逻辑过于复杂"
		};
	}
}

