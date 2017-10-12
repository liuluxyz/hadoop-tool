package hivetool;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.ReadFileHelper;

/**
 * 执行hql，并对比执行时长
 * liulu5
 * 2014-03-05
 */
public class CompareRunRime {

	private static final Log log = LogFactory.getLog(CompareRunRime.class);
	
	String db;
	String[] hqls;
	int cycle = 1;//执行次数
	
	public boolean init(String[] args){
		if(args.length == 0){
			return true;
		}
		
		for(int i=0; i<args.length; i++){
			if("-h".equals(args[i])){
				this.printUsage();
				return false;
			}
			if("-db".endsWith(args[i])){
				db = args[i+1];
				i++;
			}
			else if("-hql".equals(args[i])){
				hqls = args[i+1].split(";");
				i++;
				continue;
			}
			else if("-cycle".equals(args[i])){
				cycle = Integer.parseInt(args[i+1]);
				i++;
				continue;
			}
			else if("-file".equals(args[i])){
				boolean res = this.initcheckSQLsFromFile(args[i+1]);
				if(res == false){
					return false;
				}
				i++;
				continue;
			}
			else{
				log.info("参数不支持 : " + args[i]);
				this.printUsage();
				return false;
			}
		}
		
		log.info("db : " + db);
		return true;
	}
	
	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("说明: 查询指定的hive中的所有的表的记录数");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-db	"			+ "	指定DB");
		log.info("	-hql "			+ "	指定hql进行分析,以分号分隔");
		log.info("	-file " 		+ "	指定sql所在的文件,以分号分隔");
		log.info("	-cycle " 		+ "	指定执行的次数");
	}
	
	private boolean initcheckSQLsFromFile(String filePath){
		log.info("start initcheckSQLsFromFile...");
		String content = ReadFileHelper.readLocalFile(filePath);
		hqls = content.split(";");
		if(hqls == null || hqls.length == 0){
			log.info("hql is empty");
			return false;
		}
		log.info("end initcheckSQLsFromFile...");
		return true;
	}
	
	public void start() throws IOException{
		log.info("start...");
		long[][] duration = new long[hqls.length][cycle];
		for(int i=0; i<cycle; i++){
			log.info("开始执行第"+ (i+1) + "次,总" + cycle + "次");
			for(int j=0; j<hqls.length; j++){
				log.info("开始执行第"+ (j+1) + "个hql,总" + hqls.length + "个");
				long start = System.currentTimeMillis();
				HiveConnHelper.getInstance().executeHQL(db, hqls[j]);
				long end = System.currentTimeMillis();
				duration[j][i] = (end - start) / 1000;
				log.info("第" + (j+1) + "个hql,执行第" + (i+1) + "次时长(s):" + duration[j][i]);
			}
			log.info("结束执行第"+ (i+1) + "次,总" + cycle + "次");
		}
		
		log.info("执行结束.");
		StringBuffer result = new StringBuffer();
		result.append("统计结果:").append("\n");
		for(int i=0; i<duration.length; i++){
			result.append(Arrays.toString(duration[i])).append("\n");
		}
		log.info(result);
		
		log.info("end...");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		CompareRunRime check = new CompareRunRime();
		boolean res = check.init(args);
		if(res == false){
			return;
		}
		
		try {
			check.start();
		} catch (IOException e) {
			log.error("", e);
		}
	}

}

