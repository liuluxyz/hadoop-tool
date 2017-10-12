package analysejob.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobStatus;

import analysejob.common.AnalyseResult;
import analysejob.common.job.MRBean;

/**
 * liulu5
 * 2013-12-11
 */
public class ResultOutputHelper {
	
	private static final Log log = LogFactory.getLog(ResultOutputHelper.class);
	
	private String directory;
	
	public ResultOutputHelper(){
	}
	
	public ResultOutputHelper(String directory){
		this.directory = directory;
	}

	public String getDir(){
		if(this.directory == null){
			return "../report/" + new SimpleDateFormat("yyyy-mm-dd").format(new Date());
		}else{
			return this.directory;
		}
	}
	
	private void writeToFile(String content, String fileName){
		String filePath = getDir() + "/" + fileName;
		FileWriter write = null;
		try {
			File file = new File(filePath);
			
			File parent = file.getParentFile();
			if(!parent.exists()){
				if(!parent.mkdirs()){
					System.out.println("创建文件夹失败 : " + parent.getAbsolutePath());
				}
			}
			
			int num = 0;
			while(true){
				if(file.exists()){
					file = new File(filePath + "-" + (++num));
				}else{
					if(!file.createNewFile()){
						System.out.println("创建文件失败 : " + file.getAbsolutePath());
					}
					break;
				}
			}
			
			write = new FileWriter(file);
			write.write(content);
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}finally{
			if(write != null)
				try {
					write.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
		}
	}
	
	public void write(Date startTime, Date endTime, Map<MRBean, AnalyseResult[]> allResults, 
			Map<MRBean, AnalyseResult[]> needImproveResults, Map<String, Integer> improveStatisticsResults){
		
		String fileName = parseFilename(needImproveResults);
		
		String content = parseWriteContent(startTime, endTime, allResults, needImproveResults, improveStatisticsResults);
		writeToFile(content, fileName);
	}
	
	public void writeHQLAnalyseResult(Date startTime, Date endTime, String hql, AnalyseResult[] results){
		log.info("write HQL analyse result...");
//		String fileName = "hql-" + SimpleDateFormat.getDateTimeInstance().format(new Date()) + ".report";
		String fileName = "hql-" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + ".report";
		String content = parseHQlContent(startTime, endTime, hql, results);
		writeToFile(content, fileName);
	}
	
	private String parseFilename(Map<MRBean, AnalyseResult[]> needImproveResults){
		String startJobid = null;
		String endJobid = null;
		
		Iterator<MRBean> it = needImproveResults.keySet().iterator();
		int num = 0;
		while(it.hasNext()){
			MRBean mrbean = it.next();
			if(num == 0){
				startJobid = mrbean.getJobId();
			}
			if(num == (needImproveResults.size() - 1)){
				endJobid = mrbean.getJobId();
			}
			num++;
		}
		
		return new String(startJobid + "-" + endJobid + ".report");
	}
	
	private String parseWriteContent(Date startTime, Date endTime, Map<MRBean, AnalyseResult[]> allResults, 
			Map<MRBean, AnalyseResult[]> needImproveResults, Map<String, Integer> improveStatisticsResults){
		StringBuffer content = new StringBuffer();
		
		content.append("**********************************************************").append("\n");
		content.append("分析统计结果: ").append("\n");
		content.append("	分析时间: " + SimpleDateFormat.getDateTimeInstance().format(startTime) + " -> " + SimpleDateFormat.getDateTimeInstance().format(endTime)).append("\n");
		content.append("	分析job总数: " + allResults.size()).append("\n");
		content.append("	需要优化job数: " + needImproveResults.size()).append("\n");
		
		content.append("	需要优化job分类: ").append("\n");
		Iterator<String> stIt = improveStatisticsResults.keySet().iterator();
		while(stIt.hasNext()){
			String ruleName = stIt.next();
			int num = improveStatisticsResults.get(ruleName);
			content.append("		" + ruleName + " : " + num).append("\n");
		}
		
		content.append("**********************************************************").append("\n");
		
		Iterator<MRBean> it = needImproveResults.keySet().iterator();
		while(it.hasNext()){
			MRBean mrBean = it.next();
			AnalyseResult[] results = needImproveResults.get(mrBean);
			
			content.append("jobid: " + mrBean.getJobId()).append("\n");
			content.append("jobname: " + mrBean.getJobName()).append("\n");
			content.append("job状态: " + JobStatus.getJobRunState(mrBean.getJobRunStates())).append("\n");
			content.append("分析结果: ").append("\n");
			boolean breakLine = false;
			for(AnalyseResult result : results){
				if(breakLine == true){
					content.append("	-------------------------------------------").append("\n");
				}
				content.append(result).append("\n");
				breakLine = true;
			}
			content.append("===================================================================================").append("\n");
		}
		
		return content.toString();
	}
	
	/**
	 * 解析hql的分析结果
	 * @param startTime
	 * @param endTime
	 * @param hql
	 * @param results
	 * @return
	 */
	private String parseHQlContent(Date startTime, Date endTime, String hql, AnalyseResult[] results){
		StringBuffer content = new StringBuffer();
		
		content.append("**********************************************************").append("\n");
		content.append("分析统计结果: ").append("\n");
		content.append("	分析时间: " + SimpleDateFormat.getDateTimeInstance().format(startTime) + " -> " + SimpleDateFormat.getDateTimeInstance().format(endTime)).append("\n");
		content.append("	分析hql: " + hql).append("\n");
		content.append("	分析结果数: " + results.length).append("\n");
		content.append("**********************************************************").append("\n");
		
		content.append("分析结果: ").append("\n");
		boolean breakLine = false;
		for(AnalyseResult result : results){
			if(breakLine == true){
				content.append("	-------------------------------------------").append("\n");
			}
			content.append(result).append("\n");
			breakLine = true;
		}
		content.append("===================================================================================").append("\n");
	
		return content.toString();
	}
	
//	private void printResult(Date startTime, Date endTime, Map<MRBean, AnalyseResult[]> allResults, 
//			Map<MRBean, AnalyseResult[]> needImproveResults, Map<String, Integer> improveStatisticsResults){
//		System.out.println("**********************************************************");
//		System.out.println("分析统计结果: ");
//		System.out.println("	分析时间: " + SimpleDateFormat.getDateTimeInstance().format(startTime) + " -> " + SimpleDateFormat.getDateTimeInstance().format(endTime));
//		System.out.println("	job总数: " + allResults.size());
//		System.out.println("	需要优化job数: " + needImproveResults.size());
//		
//		System.out.println("	需要优化job分类: ");
//		Iterator<String> stIt = improveStatisticsResults.keySet().iterator();
//		while(stIt.hasNext()){
//			String ruleName = stIt.next();
//			int num = improveStatisticsResults.get(ruleName);
//			System.out.println("		" + ruleName + " : " + num);
//		}
//		
//		System.out.println("**********************************************************");
//		
//		Iterator<MRBean> it = needImproveResults.keySet().iterator();
//		while(it.hasNext()){
//			MRBean mrBean = it.next();
//			AnalyseResult[] results = needImproveResults.get(mrBean);
//			
//			System.out.println("jobid: " + mrBean.getJobId());
//			System.out.println("分析结果: ");
//			boolean breakLine = false;
//			for(AnalyseResult result : results){
//				if(breakLine == true){
//					System.out.println("	-------------------------------------------");
//				}
//				System.out.println(result);
//				breakLine = true;
//			}
//			System.out.println("===================================================================================");
//		}
//	}
}

