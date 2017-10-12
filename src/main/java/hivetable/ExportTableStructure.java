package hivetable;

import hivetool.HiveConnHelper;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.WriteFileHelper;

/**
 * 从hive库中导出表结构
 * liulu5
 * 2013-12-15
 */
public class ExportTableStructure {

	private static final Log log = LogFactory.getLog(ExportTableStructure.class);
	
	WriteFileHelper writeHelper;
	
	String db;
	String outputFilePath;
	
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
			if("-db".equals(args[i])){
				db = args[i+1];
				i++;
			}
			else if("-o".equals(args[i])){
				outputFilePath = args[i+1];
				i++;
			}
			else{
				log.info("参数不支持 : " + args[i]);
				this.printUsage();
				return false;
			}
		}
		
		if(db == null){
			log.info("请输入DB");
			this.printUsage();
			return false;
		}
		
		if(outputFilePath == null){
			outputFilePath = "./" + db + "-tables.sql";
		}
		writeHelper = new WriteFileHelper(true, outputFilePath);
		
		log.info("db : " + db);
		return true;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("说明: 从hive库中导出表结构");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-db	"		+ "	指定DB");
		log.info("	-o	"		+ "	指定表结构输出文件");
	}
	
	public void start(){
		log.info("begin ExportTableStructure.start...");
		try{
			List<String> createSqlList = getAllTableStructure();
			
			log.info("total create table sql num : " + createSqlList.size());
			writeToFile(createSqlList);	
		}catch(Exception e){
			log.error("error : ", e);
		}
		log.info("end ExportTableStructure.start...");
	}
	
	/**
	 * 获取表结构
	 * @return
	 */
	public List<String> getAllTableStructure(){
		log.info("getAllTableStructure.start...");
		
		List<String> tables = HiveConnHelper.getInstance().getAllTables(db);
		List<String> createSqlList = new ArrayList<String>();
		
		int progress = 0;
		for(String table : tables){
			List<String> tableStructure = HiveConnHelper.getInstance().executeSingleColumnQuery(db, "show create table " + table);
			
			if(tableStructure == null || tableStructure.size() == 0){
				log.info("table not find : " + table);
			}else{
				String createSql = parseCreateTableSql(tableStructure);
				createSqlList.add(createSql);
			}
			
			progress++;
			log.info("progress : [exported num : " + progress + "] [total num : " + tables.size() + "]");
		}
		
		log.info("getAllTableStructure.end...");
		return createSqlList;
	}
	
	/**
	 * 从表结构解析建表语句
	 * @param tableStructure
	 * @return
	 */
	public String parseCreateTableSql(List<String> tableStructure){
		StringBuffer createSql = new StringBuffer();
		for(int i=0; i<tableStructure.size(); i++){
			String temp = tableStructure.get(i);
			
			if(temp.trim().startsWith("ROW FORMAT DELIMITED") || temp.startsWith("LOCATION")){
				createSql.append(temp).append("\n");
				createSql.append(tableStructure.get(i+1)).append("\n");
				i += 1;
			}
			else if(temp.trim().startsWith("STORED AS")){
				String inputFormat = tableStructure.get(i+1);
				String outputFormat = tableStructure.get(i+3);
				if(inputFormat.contains("org.apache.hadoop.mapred.TextInputFormat") && 
						outputFormat.contains("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")){
					createSql.append("STORED AS ").append("\n");
					createSql.append("    TEXTFILE").append("\n");
				}
				else if(inputFormat.contains("org.apache.hadoop.hive.ql.io.RCFileInputFormat") && 
						outputFormat.contains("org.apache.hadoop.hive.ql.io.RCFileOutputFormat")){
					createSql.append("STORED AS ").append("\n");
					createSql.append("    RCFILE").append("\n");
				}
				else if(inputFormat.contains("org.apache.hadoop.mapred.SequenceFileInputFormat") && 
						outputFormat.contains("org.apache.hadoop.mapred.SequenceFileOutputFormat")){
					createSql.append("STORED AS ").append("\n");
					createSql.append("    SEQUENCEFILE").append("\n");
				}
				else{
					createSql.append(temp).append("\n");
					createSql.append(tableStructure.get(i+1)).append("\n");
					createSql.append(tableStructure.get(i+2)).append("\n");
					createSql.append(tableStructure.get(i+3)).append("\n");
				}
				i += 3;
			}
			else if(temp.trim().startsWith("TBLPROPERTIES")){
				while((i+1) < tableStructure.size()){
					if(tableStructure.get(i+1).contains(")")){
						break;
					}
					i++;
				}
				i += 1;
			}
			else{
				createSql.append(temp).append("\n");
			}
		}
		return createSql.delete(createSql.length() - 1, createSql.length()).toString();
	}
	
	private void writeToFile(List<String> createSqlList){
		log.info("start writeToFile...");
		StringBuffer content = new StringBuffer("use " + db + ";");
		for(String str : createSqlList){
			content.append(str).append(";").append("\n").append("\n");
		}
		writeHelper.writeOverwrite(content.toString());
		log.info("end writeToFile...");
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ExportTableStructure export = new ExportTableStructure();
		boolean res = export.init(args);
		if(res == false){
			return;
		}
		
		export.start();
	}

}

