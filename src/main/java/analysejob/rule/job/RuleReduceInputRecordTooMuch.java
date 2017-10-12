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
import analysejob.common.job.counter.TaskCounterKey;

/**
 * liulu5
 * 2014-03-03
 */
public class RuleReduceInputRecordTooMuch extends JobAnalyseRule{

	private static final Log log = LogFactory.getLog(RuleReduceInputRecordTooMuch.class);
	
	private long reduceMaxRecords = 10000000;//1000万
	
	public RuleReduceInputRecordTooMuch(MRAdapter mrAdapter){
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
		referInfo.put("reduce处理记录数的最大阀值(条)", reduceMaxRecords);
		referInfo.put("reduce总数", reduces.size());
		
		long[] reduceInputRecords = new long[reduces.size()];
		int num = 0;
		int invalidReduces = 0;
		for(TaskReportUDF reduce : reduces){
			long inputRecords = reduce.getTaskCounter().get(TaskCounterKey.REDUCE_OUTPUT_RECORDS.name());
			reduceInputRecords[num++] = inputRecords;
			if(inputRecords > reduceMaxRecords){
				invalidReduces++;
			}
		}
		
		if(invalidReduces > 0){
			Arrays.sort(reduceInputRecords);
			referInfo.put("超过阀值的reduce数", invalidReduces);
			referInfo.put("所有reduce的counter[REDUCE_INPUT_RECORDS]值", Arrays.toString(reduceInputRecords));
			if(mrBean.getConf(JobConfKey.mapredReduceTasks) != null){
				referInfo.put(JobConfKey.mapredReduceTasks, mrBean.getConf(JobConfKey.mapredReduceTasks));
			}
			if(mrBean.getConf(JobConfKey.hiveExecReducersBytesPerReducer) != null){
				referInfo.put(JobConfKey.hiveExecReducersBytesPerReducer, mrBean.getConf(JobConfKey.hiveExecReducersBytesPerReducer));
			}
			if(mrBean.getConf(JobConfKey.hiveQueryString) != null){
				referInfo.put("HQL", mrBean.getConf(JobConfKey.hiveQueryString));
			}
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, getDesc(), getReason(), getSuggestion(), referInfo);	
		}
			
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null, null);		
	}
	
	@Override
	public String toString() {
		return "检查reduce的处理记录数是否过大";
	}

	@Override
	public String[] getDesc() {
		return new String[]{
				"有些reduce处理记录数超过" + reduceMaxRecords + "(条)"
		};
	}

	@Override
	public String[] getSuggestion() {
		return new String[]{
				"将参数[mapred.reduce.tasks]设置为更大值",
				"将参数[hive.exec.reducers.bytes.per.reducer]设置为更小值",
				"替换order by",
				"避免笛卡尔积"
		};
	}

	@Override
	public String[] getReason() {
		return new String[]{
				"hive表列数过少，导致数据行数过多",
				"reduce的个数设置参数不合理",
				"使用了order by",
				"产生了笛卡尔积"
		};
	}
}

