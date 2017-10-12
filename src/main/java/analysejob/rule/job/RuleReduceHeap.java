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
public class RuleReduceHeap extends RuleTaskHeap{
	
	private static final Log log = LogFactory.getLog(RuleReduceHeap.class);
	
	public RuleReduceHeap(MRAdapter mrAdapter){
		this.mrAdapter = mrAdapter;
	}

	@Override
	public AnalyseResult doAnalyse(MRBean mrBean) throws Exception {
		log.info("analyse : " + mrBean.getJobId());
		
		String opts = mrBean.getConf(JobConfKey.mrChildJavaOpts);
		long childOpts = Long.parseLong(opts.substring(4, opts.length()-1)) * 1024 * 1024;
		
		List<TaskReportUDF> reduces = mrBean.getReduces();
		Map<Object, Object> referInfo = new LinkedHashMap<Object, Object>();
		parseCommonJobInfo(mrBean, referInfo);
		referInfo.put(JobConfKey.mrChildJavaOpts, childOpts);
		referInfo.put("reduce总数", reduces.size());
		
		return analyseTask(childOpts, reduces, referInfo);
	}

	@Override
	public String toString() {
		return "检查reduce使用内存是否过大";
	}

	@Override
	public String[] getDesc() {
		return  new String[]{"超过" + (invalidTaskRate*100) + "%的reduce使用了过大的内存"};
	}

	@Override
	public String[] getSuggestion() {
		return new String[0];
	}

	@Override
	public String[] getReason() {
		return new String[0];
	}
}

