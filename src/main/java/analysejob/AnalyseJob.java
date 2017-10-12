package analysejob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobStatus;

import analysejob.adapter.MRAdapter;
import analysejob.common.AnalyseResult;
import analysejob.common.job.JobConfKey;
import analysejob.common.job.MRBean;
import analysejob.rule.job.JobAnalyseRule;
import analysejob.rule.job.RuleJobDataSkew;
import analysejob.rule.job.RuleMapHeap;
import analysejob.rule.job.RuleMapInputRecordTooMuch;
import analysejob.rule.job.RuleMapReadBytesTooBig;
import analysejob.rule.job.RuleMapReadBytesTooSmall;
import analysejob.rule.job.RuleMapRunTooQuick;
import analysejob.rule.job.RuleMapRunTooSlow;
import analysejob.rule.job.RuleMapTooMany;
import analysejob.rule.job.RuleReduceHeap;
import analysejob.rule.job.RuleReduceInputRecordTooMuch;
import analysejob.rule.job.RuleReduceReadBytesTooBig;
import analysejob.rule.job.RuleReduceRunTooSlow;
import analysejob.utils.ResultOutputHelper;

/**
 * liulu5
 * 2013-11-22
 */
public class AnalyseJob {

	private static final Log log = LogFactory.getLog(AnalyseJob.class);
	
	private int cycle = 0;//指定job分析线程的间隔时间(单位为分钟)，不间断的进行分析,0表示只分析一次
	
	private MRAdapter mrAdapter ;
	private AnalyseJobOption option;
	private ResultOutputHelper outputHelper;
	
	public boolean init(String[] args){
		log.info("init...");
		boolean result = initOption(args);
		if(result == false){
			return false;
		}
		initOutputHelper();
		result = initAdapter();
		log.info("init result : " + result);
		return result;
	}
	
	public boolean initAdapter(){
		log.info("initAdapter...");
		try {
			if(option.getMrConf() != null){
				mrAdapter = new MRAdapter(option.getMrConf());
			}
			else{
				mrAdapter = new MRAdapter();
			}
		} catch (IOException e) {
			log.error("error: init MR adapter error [" + e.toString() + "]", e);
			return false;
		} catch (Exception e) {
			log.error("error: init MR adapter error [" + e.toString() + "]", e);
			return false;
		}
		return true;
	}
	
	public void initOutputHelper(){
		log.info("initOutputHelper...");
		if(option.getOutputPath() != null){
			outputHelper = new ResultOutputHelper(option.getOutputPath());
		}
		else{
			outputHelper = new ResultOutputHelper();
		}
	}
	
	public boolean initOption(String[] args){
		log.info("initOption...");
		if(option == null){
			option = new AnalyseJobOption();
		}else{
			option.clear();
		}
		
		int num = 0;
		while(num < args.length){
			try{
				if("-h".equals(args[num])){
					this.printUsage();
					return false;
				}
				else if("-cycle".equals(args[num])){
					cycle = Integer.parseInt(args[num + 1]);
					num += 2;
					continue;
				}
				else if("-jobid".equals(args[num])){
					option.setJobid(args[num + 1]);
					num += 2;
					continue;
				}
				else if("-jobname".equals(args[num])){
					if("-e".equals(args[num + 1])){
						option.setJobnameMatch(AnalyseJobOption.Match.equal);
						num++;
					}
					else if("-c".equals(args[num + 1])){
						option.setJobnameMatch(AnalyseJobOption.Match.contain);
						num++;
					}
					else{
						option.setJobnameMatch(AnalyseJobOption.Match.contain);
					}
					option.setJobname(args[num + 1]);
					num += 2;
					continue;
				}
				else if("-hiveql".equals(args[num])){
					if("-e".equals(args[num + 1])){
						option.setHiveqlMatch(AnalyseJobOption.Match.equal);
						num++;
					}
					else if("-c".equals(args[num + 1])){
						option.setHiveqlMatch(AnalyseJobOption.Match.contain);
						num++;
					}
					else{
						option.setHiveqlMatch(AnalyseJobOption.Match.contain);
					}
					option.setHiveql(args[num + 1]);
					num += 2;
					continue;
				}
//				else if("-mapmin".equals(args[num])){
//					option.setMapmin(Integer.parseInt(args[num + 1]));
//					num += 2;
//					continue;
//				}
				else if("-jobtime".equals(args[num])){
					option.setJobtime(Integer.parseInt(args[num + 1]));
					num += 2;
					continue;
				}
				else if("-conf".equals(args[num])){
					option.setMrConf(args[num + 1]);
					num += 2;
					continue;
				}
				else if("-o".equals(args[num])){
					option.setOutputPath(args[num + 1]);
					num += 2;
					continue;
				}
				else{
					log.info("error: not support parameter : " + args[num]);
					return false;
				}
			}catch(NumberFormatException e){
				log.error("error: " + args[num] + " should be number!", e);
				return false;
			}
		}
		return true;
	}
	
	public void printUsage(){
		log.info("用法: ./analyseJob.sh [-选项] [参数]");
		log.info("	e.g. ./analyseJob.sh -jobid job_201311261958_0330");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-cycle	" 		+ "	指定job分析线程的间隔时间(单位为分钟)，不间断的进行分析,0表示只分析一次");
		log.info("	-jobid	"		+ "	指定jobid进行分析");
		log.info("	-jobname -e -c"	+ "	指定jobname进行分析,[-e]:jobname完全匹配,[-c]:jobname部分匹配,默认部分匹配");
		log.info("	-hiveql -e -c"	+ "	指定hiveql进行分析,[-e]:hiveql完全匹配,[-c]:hiveql部分匹配,默认部分匹配");
//		log.info("	-mapmin	"		+ "	指定map的最小个数，低于此值的job不分析");
		log.info("	-jobtime "		+ "	指定job最低执行时长(单位为分钟)，低于此值的job不分析,默认为2min");
		log.info("	-conf	" 		+ "	指定hadoop配置文件进行分析");
		log.info("	-o	" 			+ "	指定分析结果输出目录");
	}
	
	public void start(){
		log.info("begin to analyse...");
		try {
			Date startTime = new Date();
			
			List<MRBean> mrBeans = getAnalyseJobs();
			log.info("get analyse job num : " + mrBeans.size());
			if(mrBeans.size() == 0){
				return;
			}
			
			Map<MRBean, AnalyseResult[]> allResult = new LinkedHashMap<MRBean, AnalyseResult[]>();
			for(MRBean mrBean : mrBeans){
				if(mrBean.getJobRunStates() != JobStatus.SUCCEEDED){//对运行的job进行分析
					JobAnalyseRule[] runningJobAnalyses = new JobAnalyseRule[]{
							new RuleJobDataSkew(mrAdapter)
					};
					AnalyseResult[] results = new AnalyseResult[runningJobAnalyses.length];
					for(int i=0; i<runningJobAnalyses.length; i++){
						results[i] = runningJobAnalyses[i].doAnalyse(mrBean);
					}
					
					allResult.put(mrBean, results);
				}
				else if(mrBean.getJobRunStates() == JobStatus.SUCCEEDED){//对完成的job进行分析
					JobAnalyseRule[] completeJobAnalyses = new JobAnalyseRule[]{
							new RuleMapTooMany(mrAdapter),
							new RuleMapReadBytesTooBig(mrAdapter),
							new RuleMapInputRecordTooMuch(mrAdapter),
//							new RuleMapReadBytesTooSmall(mrAdapter),
//							new RuleMapRunTooQuick(mrAdapter),
							new RuleMapRunTooSlow(mrAdapter), 
							new RuleReduceReadBytesTooBig(mrAdapter),
							new RuleReduceInputRecordTooMuch(mrAdapter),
							new RuleReduceRunTooSlow(mrAdapter), 
//							new RuleMapHeap(mrAdapter),
//							new RuleReduceHeap(mrAdapter),
							new RuleJobDataSkew(mrAdapter)
					};
					AnalyseResult[] results = new AnalyseResult[completeJobAnalyses.length];
					for(int i=0; i<completeJobAnalyses.length; i++){
						results[i] = completeJobAnalyses[i].doAnalyse(mrBean);
					}
					
					allResult.put(mrBean, results);
				}
			}
			Date endTime = new Date();
			
			Map<MRBean, AnalyseResult[]> needImproveResults = parseNeedImprove(allResult);
			Map<String, Integer> improveStatisticsResults = parseNeedImproveStatistics(needImproveResults);

			outputHelper.write(startTime, endTime, allResult, needImproveResults, improveStatisticsResults);			
		} catch (Exception e) {
			log.error("", e);
		}
		
		log.info("end to analyse...");
	}
	
	/**
	 * 获取需要分析的job
	 * @return
	 * @throws IOException
	 */
	private List<MRBean> getAnalyseJobs() throws IOException{
		List<MRBean> mrBeans = new ArrayList<MRBean>();
		boolean getAllFlag = true;
		if(option.getJobid() != null){
			MRBean mrBean = mrAdapter.getMRInfoByJobid(option.getJobid());
			if(mrBean == null){
				log.info("can not find job: " + option.getJobid());
			}else{
				mrBeans.add(mrBean);
			}
			getAllFlag = false;
		}
		if(option.getJobname() != null){
			List<MRBean> mrBeansTemp = mrAdapter.getMRInfoByJobname(option.getJobname(), option.getJobnameMatch());
			if(mrBeansTemp == null){
				log.info("can not find job: " + option.getJobname());
			}else{
				mrBeans.addAll(mrBeansTemp);
			}
			getAllFlag = false;
		}
		if(option.getHiveql() != null){
			List<MRBean> mrBeansTemp = mrAdapter.getMRInfoByHiveql(option.getHiveql(), option.getHiveqlMatch());
			if(mrBeansTemp == null){
				log.info("can not find job: " + option.getHiveql());
			}else{
				mrBeans.addAll(mrBeansTemp);
			}
			getAllFlag = false;
		}
		
		if(getAllFlag == true){
			List<MRBean> allJobs = mrAdapter.getMRInfo();
			log.info("从jobclient获取了[" + allJobs.size() + "]个job");
			int noAnalyseNum = 0;
			for(MRBean job : allJobs){//根据过滤条件过滤需要分析的job
				if(job.getConf().size() == 0){
					log.info("无法获取job conf,不分析 : " + job.getJobId());
					noAnalyseNum++;
					continue;
				}
//				if(job.getConf(JobConfKey.hiveQueryString) == null){
//					log.info("非HQL的job,不分析 : " + job.getJobId());
//					noAnalyseNum++;
//					continue;
//				}
				
				long takeTime = (job.getJobEndTime() - job.getJobStartTime()) / (1000 * 60);
				if(takeTime < option.getJobtime()){
					log.info("执行时长小于" + option.getJobtime() + "(m),不分析 : " + job.getJobId());
					noAnalyseNum++;
					continue;
				}
//				if(job.getMapNums() < option.getMapmin()){
//					log.info("map数小于" + option.getMapmin() + ",不分析 : " + job.getJobId());
//					noAnalyseNum++;
//					continue;
//				}
				log.info("");
				mrBeans.add(job);
			}
			log.info("从[" + allJobs.size() + "]个job中过滤掉了[" + noAnalyseNum + "]个job，需要分析的job个数为:["+ mrBeans.size() + "]");
		}
		return mrBeans;
	}
	
	/**
	 * parse the result that need to be improved
	 * @param allResult
	 * @return
	 */
	private Map<MRBean, AnalyseResult[]> parseNeedImprove(Map<MRBean, AnalyseResult[]> allResult){
		Map<MRBean, AnalyseResult[]> needImproveResults = new LinkedHashMap<MRBean, AnalyseResult[]>();
		
		Iterator<MRBean> it = allResult.keySet().iterator();
		while(it.hasNext()){
			MRBean key = it.next();
			AnalyseResult[] value = allResult.get(key);
			
			ArrayList<AnalyseResult> improveResults = new ArrayList<AnalyseResult>();
			for(AnalyseResult res : value){
				if(res.getResult() == AnalyseResult.Result.needimprove){
					improveResults.add(res);
				}
			}
			if(improveResults.size() > 0){
				needImproveResults.put(key, improveResults.toArray(new AnalyseResult[0]));
			}
		}
		return needImproveResults;
	}
	
	/**
	 * 解析出分析规则下对应的需优化的MR的数量
	 * @param needImproveResults
	 * @return
	 */
	private Map<String, Integer> parseNeedImproveStatistics(Map<MRBean, AnalyseResult[]> needImproveResults){
		Map<String, Integer> improveStatisticsResults = new LinkedHashMap<String, Integer>();
		
		Iterator<MRBean> it = needImproveResults.keySet().iterator();
		while(it.hasNext()){
			MRBean key = it.next();
			AnalyseResult[] value = needImproveResults.get(key);
			
			for(AnalyseResult res : value){
				if(improveStatisticsResults.containsKey(res.getRuleName())){
					int num = improveStatisticsResults.get(res.getRuleName());
					improveStatisticsResults.put(res.getRuleName(), (num+1));
				}else{
					improveStatisticsResults.put(res.getRuleName(), 1);
				}
			}
		}
		return improveStatisticsResults;
	}
	
	public int getCycle() {
		return cycle;
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		AnalyseJob analyse = new AnalyseJob();
		boolean result = analyse.init(args);
		if(result == false){
			return;
		}
		
		if(analyse.getCycle() == 0){
			analyse.start();
		}
		else{
			log.info("开始循环分析, 间隔时间(m)为 ：" + analyse.getCycle());
			while(true){
				analyse.start();
				try {
					log.info("start sleep...");
					Thread.sleep(analyse.getCycle() * 60 * 1000);
					log.info("end sleep...");
				} catch (InterruptedException e) {
					log.error("", e);
				}
			}
		}
		
	}
}