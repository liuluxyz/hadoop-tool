package analysejob;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import analysejob.common.AnalyseResult;
import analysejob.rule.hql.HQLAnalyseRule;
import analysejob.rule.hql.RuleHQLDataSkew;
import analysejob.utils.ResultOutputHelper;

/**
 * 对hql分析
 * @author liulu5
 */
public class AnalyseHQL {

	private static final Log log = LogFactory.getLog(AnalyseHQL.class);
	
	private AnalyseHQLOption option;
	private ResultOutputHelper outputHelper;
	
	public boolean init(String[] args){
		boolean result = initOption(args);
		if(result == false){
			return false;
		}
		initOutputHelper();
		return true;
	}
	
	public void initOutputHelper(){
		if(option.getOutputPath() != null){
			outputHelper = new ResultOutputHelper(option.getOutputPath());
		}
		else{
			outputHelper = new ResultOutputHelper();
		}
	}
	
	public boolean initOption(String[] args){
		
		if(option == null){
			option = new AnalyseHQLOption();
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
				else if("-hiveserver".equals(args[num])){
					option.setHiveserver(args[num + 1]);
					num += 2;
					continue;
				}
				else if("-port".equals(args[num])){
					option.setPort(Integer.parseInt(args[num + 1]));
					num += 2;
					continue;
				}
				else if("-db".equals(args[num])){
					option.setDb(args[num + 1]);
					num += 2;
					continue;
				}
				else if("-hql".equals(args[num])){
					option.setHql(args[num + 1]);
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
					this.printUsage();
					return false;
				}
			}catch(NumberFormatException e){
				log.error("error: " + args[num] + " should be number!", e);
				this.printUsage();
				return false;
			}
		}
		if(option.getHql() == null){
			log.warn("请指定HQL");
			this.printUsage();
			return false;
		}
		return true;
	}
	
	public void printUsage(){
		log.info("用法: hadoop jar xxx.jar [-选项] [参数]");
		log.info("	e.g. hadoop jar tool.jar -hql 'select xxx from ...'");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-hiveserver	"	+ "	指定hiveserver所在的IP,默认为本机");
		log.info("	-port	"		+ "	指定hiveserver的端口,默认为10000");
		log.info("	-db	"			+ "	指定HQL运行所在DB,默认为default");
		log.info("	-hql	"		+ "	指定hql进行分析");
		log.info("	-o	" 			+ "	指定分析结果输出目录");
	}
	
	public void start(){
		log.info("begin to analyse...");
		try {
			Date startTime = new Date();
			HQLAnalyseRule[] analyseRules = new HQLAnalyseRule[]{
					new RuleHQLDataSkew()
			};
			
			AnalyseResult[] results = new AnalyseResult[analyseRules.length];
			for(int i=0; i<analyseRules.length; i++){
				results[i] = analyseRules[i].doAnalyse(option);
			}
			
			Date endTime = new Date();
			outputHelper.writeHQLAnalyseResult(startTime, endTime, option.getHql(), results);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("exception : " + e.toString(), e);
		}
		
		log.info("end to analyse...");
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		AnalyseHQL analyse = new AnalyseHQL();
		boolean result = analyse.init(args);
		if(result == false){
			return;
		}
		analyse.start();
	}
}