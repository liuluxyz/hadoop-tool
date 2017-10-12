package analysejob.rule.job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TIPStatus;

import analysejob.adapter.MRAdapter;
import analysejob.common.AnalyseResult;
import analysejob.common.job.JobConfKey;
import analysejob.common.job.MRBean;
import analysejob.common.job.TaskReportUDF;
import analysejob.common.job.counter.FSCounterKey;
import analysejob.common.job.counter.TaskCounterKey;

/**
 * 分析job：数据倾斜
1. 症状：
    (1). 任务进度长时间维持在99%（或100%），只有少量（1个或几个）reduce子任务未完成。
    (2). 查看未完成的子任务，可以看到本地读写数据量积累非常大，通常超过10GB可以认定为发生数据倾斜。
2. 倾斜度：
    (1). 平均记录数超过50w且最大记录数是超过平均记录数的4倍。
    (2). 最长时长比平均时长超过4分钟，且最大时长超过平均时长的2倍

 * liulu5
 * 2013-11-21
 */
public class RuleJobDataSkew extends JobAnalyseRule{

	private static final Log log = LogFactory.getLog(RuleJobDataSkew.class);
	
	private final long bytesWrittenThreshold = 10L * 1024L * 1024L;//10GB，reduce写数据量的阀值（超过阀值可以认定为发生数据倾斜）,单位KB

	public RuleJobDataSkew(MRAdapter mrAdapter){
		this.mrAdapter = mrAdapter;
	}

	@Override
	public AnalyseResult doAnalyse(MRBean mrBean) throws Exception {
		log.info("analyse : " + mrBean.getJobId());
		
		List<TaskReportUDF> maps = mrBean.getMaps();
		for(TaskReportUDF map : maps){
			if(map.getStatus().ordinal() == TIPStatus.RUNNING.ordinal()){//若map未完成，不检查reduce是否倾斜
				return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[]{"map未完成"}, null, null);
			}
		}
		
		List<TaskReportUDF> reduces = mrBean.getReduces();
		for(TaskReportUDF reduce : reduces){
			if(reduce.getStartTime() == 0){
				return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[]{"尚有reduce未开始"}, null, null);
			}
		}
		
		Map<Object, Object> referInfo = new LinkedHashMap<Object, Object>();
		parseCommonJobInfo(mrBean, referInfo);
		referInfo.put("reduce总数", reduces.size());
		referInfo.put("reduce写数据量的倾斜阀值(KB)", bytesWrittenThreshold);
		
		List<String> desc = new ArrayList<String>();
		
		//1. 分析reduce是否写数据量过大
		long[] allBytesWritten = parseAllBytesWritten(reduces);
		long averageWritten = parseAverageBytesWritten(reduces);
		referInfo.put("reduce写数据量平均值(KB)", averageWritten);
		log.info("average written(KB) : " + averageWritten);
		log.info("bytesWrittenThreshold(KB) : " + bytesWrittenThreshold);
		int writenMoreThanThreshold = 0;//超过阀值的reduce个数
		int writenToMuchReduce = 0;//超过平均值3倍的reduce个数
		for(TaskReportUDF reduce : reduces){
			long bytesWritten = Long.parseLong(reduce.getFsCounter().get(FSCounterKey.FILE_BYTES_WRITTEN.name()).toString()) / 1000;
			if(bytesWritten > bytesWrittenThreshold){
				writenMoreThanThreshold++;
				log.info("reduce写数据量超过阀值(KB)：" + bytesWritten);
			}
			
			if(averageWritten != 0){
				double rate = bytesWritten / averageWritten;
				if(rate > 3){
					writenToMuchReduce++;
					log.info("reduce写数据量超过平均值(KB)：" + bytesWritten);
				}
			}
		}
		if(writenMoreThanThreshold > 0){
			referInfo.put("reduce写数据量(KB)", Arrays.toString(allBytesWritten));
			desc.add(writenMoreThanThreshold + "个reduce的写数据量超过10G");
		}
		if(writenToMuchReduce > 0){
			referInfo.put("reduce写数据量(KB)", Arrays.toString(allBytesWritten));
			desc.add(writenToMuchReduce + "个reduce的写数据量超过平均值的3倍");
		}
		
		//2. 分析reduce处理记录数是否过大
		long[] allRecords = parseAllRecords(reduces);
		long averageRecord = parseAverageRecords(reduces);
		int recordsToMuchReduce = 0;
		if(averageRecord > 50 * 10000){
			for(TaskReportUDF reduce : reduces){
				long records = Long.parseLong(reduce.getTaskCounter().get(TaskCounterKey.REDUCE_INPUT_RECORDS.name()).toString());
				if(averageRecord != 0){
					double rate = records / averageRecord;
					if(rate > 4){
						recordsToMuchReduce++;
					}
				}
			}
		}
		if(recordsToMuchReduce > 0){
			referInfo.put("reduce输入记录数", Arrays.toString(allRecords));
			desc.add(recordsToMuchReduce + "个reduce的输入记录数超过平均值的4倍");
		}
		
		//3. 分析执行过长的reduce
		long[] allTakeTime = this.parseAllTakeTime(reduces);
		long averageTime = parseAverageTime(reduces);
		int runLongReduce = 0;
		if(averageTime > 0){
			for(TaskReportUDF reduce : reduces){
				long reduceTime = 0;
				if(reduce.getFinishTime() == 0){
					reduceTime = System.currentTimeMillis() - reduce.getStartTime();
				}else{
					reduceTime = reduce.getFinishTime() - reduce.getStartTime();
				}
				//reduce执行时长比平均时长超过4分钟，且reduce执行时长超过平均时长的2倍
				if(millisecondToMinute(reduceTime - averageTime) > 4 && (reduceTime / averageTime) > 2){
					runLongReduce++;
				}
			}
		}
		if(runLongReduce > 0){
			referInfo.put("reduce执行时长(s)", Arrays.toString(allTakeTime));
			desc.add(runLongReduce + "个reduce的执行时间过长(比平均时长超过4分钟,且超过平均时长的2倍)");
		}
		
		if(desc.size() > 0){
			if(mrBean.getConf(JobConfKey.hiveQueryString) != null){
				referInfo.put("HQL", mrBean.getConf(JobConfKey.hiveQueryString));
			}
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, desc.toArray(new String[0]), getReason(), getSuggestion(), referInfo);
		}
		
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], null, null);
	}
	
	/**
	 * 计算reduce的平均写数据量:只计算完成的reduce,单位KB
	 * @param reduces
	 * @return
	 */
	private long parseAverageBytesWritten(List<TaskReportUDF> reduces){
		long total = 0;
		int num = 0;
		for(TaskReportUDF reduce : reduces){
			if(reduce.getStatus().ordinal() == TIPStatus.COMPLETE.ordinal()){
				total += reduce.getFsCounter().get(FSCounterKey.FILE_BYTES_WRITTEN.name()) / 1000;
				num++;
			}
		}
		if(num == 0){
			return 0;
		}
		return total / num;
	}
	
	/**
	 * 解析出所有的reduce的写数据量,单位KB
	 * @param reduces
	 * @return
	 */
	private long[] parseAllBytesWritten(List<TaskReportUDF> reduces){
		long[] all = new long[reduces.size()];
		for(int i=0; i<reduces.size(); i++){
			all[i] = reduces.get(i).getFsCounter().get(FSCounterKey.FILE_BYTES_WRITTEN.name()) / 1000;
		}
		Arrays.sort(all);
		return all;
	}
	
	/**
	 * 解析reduce的平均写入记录数
	 * @param reduces
	 * @return
	 */
	private long parseAverageRecords(List<TaskReportUDF> reduces){
		long total = 0;
		int num = 0;
		for(TaskReportUDF reduce : reduces){
			if(reduce.getStatus().ordinal() == TIPStatus.COMPLETE.ordinal()){
				total += reduce.getTaskCounter().get(TaskCounterKey.REDUCE_INPUT_RECORDS.name());
				num++;
			}
		}
		if(num == 0){
			return 0;
		}
		return total / num;
	}
	
	/**
	 * 解析出所有的reduce的写入记录数
	 * @param reduces
	 * @return
	 */
	private long[] parseAllRecords(List<TaskReportUDF> reduces){
		long[] all = new long[reduces.size()];
		for(int i=0; i<reduces.size(); i++){
			all[i] = reduces.get(i).getTaskCounter().get(TaskCounterKey.REDUCE_INPUT_RECORDS.name());
		}
		Arrays.sort(all);
		return all;
	}
	
	/**
	 * 解析reduce的执行平均时间:只计算完成的reduce
	 * @param reduces
	 * @return
	 */
	private long parseAverageTime(List<TaskReportUDF> reduces){
		long total = 0;
		int num = 0;
		for(TaskReportUDF reduce : reduces){
			if(reduce.getStatus().ordinal() == TIPStatus.COMPLETE.ordinal()){
				total += reduce.getFinishTime() - reduce.getStartTime();
				num++;
			}
		}
		if(num == 0){
			return 0;
		}
		return total / num;
	}
	
	/**
	 * 解析所有reduce的执行时长
	 * @param reduces
	 * @return
	 */
	private long[] parseAllTakeTime(List<TaskReportUDF> reduces){
		long[] all = new long[reduces.size()];
		for(int i=0; i<reduces.size(); i++){
			if(reduces.get(i).getStatus().ordinal() == TIPStatus.COMPLETE.ordinal()){
				all[i] = (reduces.get(i).getFinishTime() - reduces.get(i).getStartTime()) / 1000;
			}
			else{
				all[i] = (System.currentTimeMillis() - reduces.get(i).getStartTime()) / 1000;
			}
		}
		Arrays.sort(all);
		return all;
	}
	
	private long millisecondToMinute(long time){
		return time / (1000 * 60);
	}

	@Override
	public String toString() {
		return "检查job的reduce是否发生数据倾斜";
	}

	@Override
	public String[] getDesc() {
		// TODO Auto-generated method stub
		return new String[]{"发生数据倾斜"};
	}

	@Override
	public String[] getSuggestion() {
		// TODO Auto-generated method stub
		return new String[]{
				"若数据为非法数据，则过滤或者忽略倾斜数据",
				"若数据需要保留，则使用随机数进行替换"
		};
	}

	@Override
	public String[] getReason() {
		// TODO Auto-generated method stub
		return new String[]{"发生数据倾斜"};
	}
}

