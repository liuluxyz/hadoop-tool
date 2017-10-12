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
 * 2014-03-13
 */
public class RuleMapInputRecordTooMuch extends JobAnalyseRule{

	private static final Log log = LogFactory.getLog(RuleMapInputRecordTooMuch.class);
	
	private long mapMaxRecords = 10000000;//1000万
	
	public RuleMapInputRecordTooMuch(MRAdapter mrAdapter){
		this.mrAdapter = mrAdapter;
	}

	@Override
	public AnalyseResult doAnalyse(MRBean mrBean) throws Exception {
		log.info("analyse : " + mrBean.getJobId());
		
		List<TaskReportUDF> maps = mrBean.getMaps();
		if(maps.size() <= 0){
			return new AnalyseResult(toString(), AnalyseResult.Result.fail, new String[]{"no map"}, null, null);
		}
		
		Map<Object, Object> referInfo = new LinkedHashMap<Object, Object>();
		parseCommonJobInfo(mrBean, referInfo);
		if(mrBean.getConf(JobConfKey.mapredMaxSplitSize) != null)
			referInfo.put(JobConfKey.mapredMaxSplitSize, mrBean.getConf(JobConfKey.mapredMaxSplitSize));
		if(mrBean.getConf(JobConfKey.mapredMinSplitSize) != null)
			referInfo.put(JobConfKey.mapredMinSplitSize, mrBean.getConf(JobConfKey.mapredMinSplitSize));
		if(mrBean.getConf(JobConfKey.mapredMinSplitSizePerNode) != null)
			referInfo.put(JobConfKey.mapredMinSplitSizePerNode, mrBean.getConf(JobConfKey.mapredMinSplitSizePerNode));
		if(mrBean.getConf(JobConfKey.mapredMinSplitSizePerRack) != null)
			referInfo.put(JobConfKey.mapredMinSplitSizePerRack, mrBean.getConf(JobConfKey.mapredMinSplitSizePerRack));
		if(mrBean.getConf(JobConfKey.hiveInputFormat) != null)
			referInfo.put(JobConfKey.hiveInputFormat, mrBean.getConf(JobConfKey.hiveInputFormat));
		referInfo.put("map处理记录数的最大阀值(条)", mapMaxRecords);
		referInfo.put("map总数", maps.size());
		
		long[] mapInputRecords = new long[maps.size()];
		int num = 0;
		int invalidMaps = 0;
		for(TaskReportUDF map : maps){
			long inputRecords = map.getTaskCounter().get(TaskCounterKey.MAP_INPUT_RECORDS.name());
			mapInputRecords[num++] = inputRecords;
			if(inputRecords > mapMaxRecords){
				invalidMaps++;
			}
		}
		
		if(invalidMaps > 0){
			Arrays.sort(mapInputRecords);
			referInfo.put("超过阀值的map数", invalidMaps);
			referInfo.put("所有map的counter[MAP_INPUT_RECORDS]值", Arrays.toString(mapInputRecords));
			if(mrBean.getConf(JobConfKey.hiveQueryString) != null){
				referInfo.put("HQL", mrBean.getConf(JobConfKey.hiveQueryString));
			}
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, getDesc(), getReason(), getSuggestion(), referInfo);	
		}
			
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null, null);		
	}
	
	@Override
	public String toString() {
		return "检查map的处理记录数是否过大";
	}

	@Override
	public String[] getDesc() {
		return new String[]{
				"有些map处理记录数超过" + mapMaxRecords + "(条)"
		};
	}

	@Override
	public String[] getSuggestion() {
		return new String[]{
				"检查相关参数设置是否合理",
				"检查输入文件是否太大且不可分割，若太大，可以在这些文件生成时，进行分拆",
				"减少数据量处理量"
		};
	}

	@Override
	public String[] getReason() {
		return new String[]{
				"参数设置不合理: {mapred.max.split.size, mapred.min.split.size, mapred.min.split.size.per.node, mapred.min.split.size.per.rack, hive.input.format}",
				"job处理的输入文件太大且不可分割",
				"hive表列数过少"
		};
	}
}

