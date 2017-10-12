package hivetable;

import hivetool.HiveConnHelper;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.ResultData;

/**
 * 从源库向目标库导入表结构
 * liulu5
 * 2013-12-15
 */
public class ImportTableStructure {

	private static final Log log = LogFactory.getLog(ImportTableStructure.class);
	
	String sourceDB;
	String targetDB;
	boolean force = false;
	
	public boolean init(String[] args){
		if(args.length == 0){
			log.info("请输入参数");
			this.printUsage();
			return false;
		}
		
		for(int i=0; i<args.length; i++){
			if("-h".equals(args[i])){
				this.printUsage();
				return false;
			}
			if("-s".equals(args[i])){
				sourceDB = args[i+1];
				i++;
			}
			else if("-t".equals(args[i])){
				targetDB = args[i+1];
				i++;
			}
			else if("-f".equals(args[i])){
				force = true;
			}
			else{
				log.info("参数不支持 : " + args[i]);
				this.printUsage();
				return false;
			}
		}
		
		if(sourceDB == null){
			log.info("请输入源DB");
			this.printUsage();
			return false;
		}
		if(targetDB == null){
			log.info("请输入目标DB");
			this.printUsage();
			return false;
		}
		
		log.info("sourceDB : " + sourceDB);
		log.info("targetDB : " + targetDB);
		log.info("force : " + force);
		return true;
	}
	
	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("说明: 从源库向目标库导入表结构");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-s	"		+ "	指定源DB");
		log.info("	-t	"		+ "	指定目标DB");
		log.info("	-f	"		+ "	若表已存在，则先删除");
	}
	
	public void start(){
		log.info("begin ImportTableStructure.start...");
		
		ExportTableStructure export = new ExportTableStructure();
		export.setDb(sourceDB);
		List<String> createSqlList = export.getAllTableStructure();
		
		int successNum = 0;
		int failNum = 0;
		for(String sql : createSqlList){
			String targetSql = parseTargetSql(sql);
			ResultData result = null;
			if(targetSql == null){
				result = new ResultData(false, "parseTargetSql error");
				log.warn("parseTargetSql error : " + sql);
			}else{
				String tableName = targetSql.toLowerCase().substring(
						targetSql.toLowerCase().indexOf("table")+5, targetSql.toLowerCase().indexOf("("));
				log.info("tableName : " + tableName);
				
				if(force == true){
					String dropSql = "drop table if exists " + tableName;
					HiveConnHelper.getInstance().executeHQL(targetDB, dropSql);
				}
				result = HiveConnHelper.getInstance().executeHQL(targetDB, targetSql);
			}
			
			if(result.isResult()){
				successNum++;
			}else{
				failNum++;
			}

			log.info("execute create table sql result : " + result.isResult() + "]");
			log.info("progress : [successNum : " + successNum + "] [failNum : " + failNum + "] [total num : " + createSqlList.size() + "]");
		}
		
		log.info("end ImportTableStructure.start...");
	}
	
	/**
	 * 将源建表sql转换为目标sql：主要修改location
	 * @param sourceSql
	 * @return
	 */
	private String parseTargetSql(String sourceSql){
		String[] sqlStructure = sourceSql.split("\n");
		for(int i=0; i<sqlStructure.length; i++){
			 if(sqlStructure[i].trim().startsWith("LOCATION")){
				 String oldLocation = sqlStructure[i+1];
				 
				 String newLocation = oldLocation.replaceFirst(sourceDB, targetDB);
				 String newSql = sourceSql.replaceFirst(oldLocation, newLocation);
				 
				 log.info("source sql : " + sourceSql);
				 log.info("target sql : " + newSql);
				 return newSql;
			 }
		}
		return null;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ImportTableStructure importTable = new ImportTableStructure();
		boolean res = importTable.init(args);
		if(res == false){
			return;
		}
		importTable.start();
	}

}

