package analysejob.rule.job;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import analysejob.adapter.MRAdapter;
import analysejob.common.AnalyseResult;
import analysejob.common.job.MRBean;
import analysejob.common.job.TaskReportUDF;
import analysejob.common.job.counter.TaskCounterKey;

/**
 * liulu5
 * 2013-11-21
 */
public abstract class RuleTaskHeap extends JobAnalyseRule{

	private final double heapUseMaxRate = 0.9;//每个task如果使用了多大的内存，则说明在GC
	protected final double invalidTaskRate = 0.5;//使用了大内存的task所占用总task的比例，超过此比例，则认为整个job需要优化
	
	protected AnalyseResult analyseTask(long childOpts, List<TaskReportUDF> tasks, final Map<Object, Object> referInfo){
		int useTooBigHeapNum = 0;
		long[] heaps = new long[tasks.size()];
		int num = 0;
		for(TaskReportUDF task : tasks){
			long committedHeap = task.getTaskCounter().get(TaskCounterKey.COMMITTED_HEAP_BYTES.name());
			double rate = (double)committedHeap / (double)childOpts;
			if(rate > heapUseMaxRate){
				useTooBigHeapNum++;
			}
			heaps[num++] = committedHeap;
		}
		double offsetRate = Math.round((double)useTooBigHeapNum / (double)tasks.size() * 100) / 100.0;
		
		referInfo.put("task使用的内存与设定内存之间的偏移率设定", heapUseMaxRate);
		referInfo.put("所有task的counter[COMMITTED_HEAP_BYTES]值", Arrays.toString(heaps));
		referInfo.put("超过阀值的task占比", offsetRate*100 + "%");
		
		if(offsetRate > invalidTaskRate){
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, getDesc(), getReason(), getSuggestion(), referInfo);
		}
		
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null);
	}
}

