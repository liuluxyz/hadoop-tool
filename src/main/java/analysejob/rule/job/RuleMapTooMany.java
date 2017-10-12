package analysejob.rule.job;

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
 * 检查map的个数是否过多
 * liulu5
 * 2014-03-05
 */
public class RuleMapTooMany extends JobAnalyseRule{

	private static final Log log = LogFactory.getLog(RuleMapTooMany.class);
	
	private final int mapNumThreshold = 1000;//map个数阀值
	
	public RuleMapTooMany(MRAdapter mrAdapter){
		this.mrAdapter = mrAdapter;
	}
	
	@Override
	public AnalyseResult doAnalyse(MRBean mrBean) throws Exception {
		log.info("analyse : " + mrBean.getJobId());
		
		String blockSizeStr = mrBean.getConf(JobConfKey.dfsBlockSize);
		Long blockSize =Long.parseLong(blockSizeStr);
		
		List<TaskReportUDF> maps = mrBean.getMaps();
		
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
		
		referInfo.put("map总数", maps.size());
		referInfo.put("map个数阀值", mapNumThreshold);
		
		if(maps.size() > mapNumThreshold){
			if(mrBean.getConf(JobConfKey.hiveQueryString) != null){
				referInfo.put("HQL", mrBean.getConf(JobConfKey.hiveQueryString));
			}
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, getDesc(), getReason(), getSuggestion(), referInfo);
		}
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null);
	}

	@Override
	public String toString() {
		return "检查map数是否过多";
	}

	@Override
	public String[] getDesc() {
		return new String[]{"map数超过阀值" + mapNumThreshold};
	}

	@Override
	public String[] getSuggestion() {
		return new String[]{
				"检查相关参数设置是否合理",
				"检查hive语句的分区条件"
		};
	}

	@Override
	public String[] getReason() {
		return new String[]{
				"参数设置不合理: {mapred.max.split.size, mapred.min.split.size, mapred.min.split.size.per.node, mapred.min.split.size.per.rack, hive.input.format}",
				"hive语句是否添加了分区条件"
		};
	}
}

