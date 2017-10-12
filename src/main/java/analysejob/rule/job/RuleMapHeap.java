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
 * liulu5
 * 2013-11-21
 */
public class RuleMapHeap extends RuleTaskHeap{
	
	private static final Log log = LogFactory.getLog(RuleMapHeap.class);
	
	public RuleMapHeap(MRAdapter mrAdapter){
		this.mrAdapter = mrAdapter;
	}
	
	@Override
	public AnalyseResult doAnalyse(MRBean mrBean) throws Exception {
		log.info("analyse : " + mrBean.getJobId());
		
		String opts = mrBean.getConf(JobConfKey.mrChildJavaOpts);
		long childOpts = Long.parseLong(opts.substring(4, opts.length()-1)) * 1024 * 1024;
		
		List<TaskReportUDF> maps = mrBean.getMaps();

		Map<Object, Object> referInfo = new LinkedHashMap<Object, Object>();
		parseCommonJobInfo(mrBean, referInfo);
		referInfo.put(JobConfKey.mrChildJavaOpts, childOpts);
		referInfo.put("map总数", maps.size());
		
		return analyseTask(childOpts, maps, referInfo);
	}

	@Override
	public String toString() {
		return "检查map使用内存是否过大";
	}

	@Override
	public String[] getDesc() {
		// TODO Auto-generated method stub
		return new String[]{"超过" + (invalidTaskRate*100) + "%的map使用了过大的内存"};
	}

	@Override
	public String[] getSuggestion() {
		// TODO Auto-generated method stub
		return new String[0];
	}

	@Override
	public String[] getReason() {
		// TODO Auto-generated method stub
		return new String[0];
	}
}

