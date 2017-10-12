package analysejob.rule.job;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;

import analysejob.adapter.MRAdapter;
import analysejob.common.AnalyseResult;
import analysejob.common.job.JobConfKey;
import analysejob.common.job.MRBean;
import analysejob.common.job.TaskReportUDF;
import analysejob.common.job.counter.FSCounterKey;

/**
 * 检查map的处理数据量是否可优化
 * liulu5
 * 2013-11-21
 */
public class RuleMapReadBytesTooSmall extends JobAnalyseRule{

	private static final Log log = LogFactory.getLog(RuleMapReadBytesTooSmall.class);
	
	private double mapReadBytesOffsetRate = 0.7;//map读取的bytes与规定的阀值之间的偏移率,小于此值，则认为此map所read的bytes偏离规定的阀值
	private double offsetNumRate = 0.3;//偏离规定数据量的task占总task的比率阀值，超过此值，则认为job的整体运行不正常
	
	public RuleMapReadBytesTooSmall(MRAdapter mrAdapter){
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
		referInfo.put("map读数据量的阀值(B)", blockSize);
		referInfo.put("map读取数据量与阀值之间的偏移率设定", mapReadBytesOffsetRate);
		
		//rule: map处理的数据量相比块大小过小，则需要优化
		Long[] values = new Long[maps.size()];
		int invalidNum = 0;
		for(int i=0; i<maps.size(); i++){
			values[i] = maps.get(i).getFsCounter().get(FSCounterKey.HDFS_BYTES_READ.name());
			double rate = (double)values[i] / (double)blockSize;
			if(rate < mapReadBytesOffsetRate){
				invalidNum++;
			}
		}
		double offsetRate = Math.round((double)invalidNum / (double)maps.size() * 100) / 100.0;
		
		referInfo.put("超过阀值的map数", invalidNum);
		referInfo.put("超过阀值的map数占比", offsetRate*100 + "%");
		referInfo.put("所有map的[BYTES_READ]值", Arrays.toString(values));
		
		if(offsetRate > offsetNumRate){
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, getDesc(), getReason(), getSuggestion(), referInfo);
		}
		
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null);
	}

	@Override
	public String toString() {
		return "检查map的处理数据量是否过小";
	}

	@Override
	public String[] getDesc() {
		return new String[]{"超过" + (offsetNumRate*100) + "%的map处理数据量远小于dfs.blocksize"};
	}

	@Override
	public String[] getSuggestion() {
		return new String[]{
				"检查相关参数设置是否合理",
				"检查输入文件是否碎小且不可分割，若碎小，可以在这些文件生成时，进行合并"
		};
	}

	@Override
	public String[] getReason() {
		return new String[]{
				"参数设置不合理: {mapred.max.split.size, mapred.min.split.size, mapred.min.split.size.per.node, mapred.min.split.size.per.rack, hive.input.format}",
				"job处理的输入文件碎小且不可分割"
		};
	}
}

