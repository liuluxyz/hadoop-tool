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
import analysejob.common.job.counter.FSCounterKey;

/**
 * liulu5
 * 2013-11-21
 */
public class RuleReduceReadBytesTooBig extends JobAnalyseRule{

	private static final Log log = LogFactory.getLog(RuleReduceReadBytesTooBig.class);
	
	private long reduceMaxRead = 1024;//1G,单位为m
	
	public RuleReduceReadBytesTooBig(MRAdapter mrAdapter){
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
		referInfo.put("reduce处理数据量的最大阀值(m)", reduceMaxRead);
		referInfo.put("reduce总数", reduces.size());
		
		long[] reduceReadBytes = new long[reduces.size()];
		int num = 0;
		int invalidReduces = 0;
		for(TaskReportUDF reduce : reduces){
			long readBytes = reduce.getFsCounter().get(FSCounterKey.FILE_BYTES_READ.name());
			reduceReadBytes[num++] = readBytes;
			if(readBytes > (reduceMaxRead * 1024 * 1024)){
				invalidReduces++;
			}
		}
		
		if(invalidReduces > 0){
			Arrays.sort(reduceReadBytes);
			referInfo.put("超过阀值的reduce数", invalidReduces);
			referInfo.put("所有reduce的counter[FILE_BYTES_READ]值", Arrays.toString(reduceReadBytes));
			if(mrBean.getConf(JobConfKey.hiveQueryString) != null){
				referInfo.put("HQL", mrBean.getConf(JobConfKey.hiveQueryString));
			}
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, getDesc(), getReason(), getSuggestion(), referInfo);	
		}
			
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null, null);		
	}
	
	@Override
	public String toString() {
		return "检查reduce的处理数据量是否过大";
	}

	@Override
	public String[] getDesc() {
		return new String[]{
				"有些reduce处理数据量超过" + reduceMaxRead + "(m)"
		};
	}

	@Override
	public String[] getSuggestion() {
		return new String[]{
				"将参数[mapred.reduce.tasks]设置为更大值",
				"替换order by"
		};
	}

	@Override
	public String[] getReason() {
		return new String[]{
				"reduce的个数设置不合理",
				"使用了order by"
		};
	}
}

