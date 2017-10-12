package autoinsertoverwrite;

import hivetool.HiveConnHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.ResultData;

/**
 * 自动将源DB中的所有表数据使用【insert overwrite】导入到目标DB，含有特殊导入规则，只用于山东经分云化
 * liulu5
 * 2013-12-15
 */
public class AutoInsert {

	private static final Log log = LogFactory.getLog(AutoInsert.class);
	
	private final RecordInfo recordInfo;
	private int parallel = 20;//并行执行的数量
	private String config;
	
	private static final String[] noInsertTable = new String[]{
		"cdr_newbusi_ds"
	};
	
	public AutoInsert(){
		recordInfo = new RecordInfo();
	}
	
	public boolean init(String[] args){
		if(args.length == 0){
			return true;
		}
		try{
			for(int i=0; i<args.length; i++){
				if("-h".equals(args[i])){
					this.printUsage();
					return false;
				}
				if("-insert".equals(args[i])){
					recordInfo.setInsert(true);
				}
				else if("-s".equals(args[i])){
					recordInfo.setSourceDB(args[i+1]);
					i++;
				}
				else if("-t".equals(args[i])){
					recordInfo.setTargetDB(args[i+1]);
					i++;
				}
				else if("-p".equals(args[i])){
					parallel = Integer.parseInt(args[i+1]);
					i++;
				}
				else if("-d".equals(args[i])){
					if(args[i+1] == null || !args[i+1].contains("=")){
						log.info("参数错误 : " + args[i+1]);
						this.printUsage();
						return false;
					}
					config = args[i+1];
					i++;
				}
				else{
					log.info("参数不支持 : " + args[i]);
					this.printUsage();
					return false;
				}
			}
			
			if(recordInfo.getSourceDB() == null){
				log.info("请输入: sourcedb");
				this.printUsage();
				return false;
			}
			if(recordInfo.getTargetDB() == null){
				log.info("请输入: targetDB");
				this.printUsage();
				return false;
			}
			if(parallel < 1){
				log.info("参数【并行执行数量】不能小于1 ：" + parallel);
				this.printUsage();
				return false;
			}
		}catch(Exception e){
			e.printStackTrace();
			log.info("参数输入错误");
			this.printUsage();
			return false;
		}
		log.info("user input parameter info : " + recordInfo.getParameterInfo());
		return true;
	}
	
	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("说明: 自动将源DB中的所有表数据使用【insert overwrite】导入到目标DB，含有特殊导入规则，只用于山东经分云化");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-insert	"		+ "	是否执行insert操作，若不指定，则只做check");
		log.info("	-s	"		+ "	指定源DB");
		log.info("	-t	"		+ "	指定目标DB");
		log.info("	-p	"		+ "	指定并行执行的数量");
		log.info("	-d	"		+ "	指定参数配置:<property=value>");
	}

	public String getConfig() {
		return config;
	}
	
	public void start(){
		log.info("AutoInsert start...");
		// auto create table
		
		List<String> sourceTables = HiveConnHelper.getInstance().getAllTables(recordInfo.getSourceDB());
		recordInfo.addStatisticsInfo("table num in " + recordInfo.getSourceDB(), sourceTables.size());
		log.info("table num in " + recordInfo.getSourceDB() + " : " + sourceTables.size());
		
		List<String> targetTables = HiveConnHelper.getInstance().getAllTables(recordInfo.getTargetDB());
		recordInfo.addStatisticsInfo("table num in " + recordInfo.getTargetDB(), targetTables.size());
		log.info("table num in " + recordInfo.getTargetDB() + " : " + targetTables.size());
		
		List<String> existTables = parseExistTable(sourceTables, targetTables);
		recordInfo.addStatisticsInfo("exist table num", existTables.size());
		recordInfo.addStatisticsInfo("not exist table num", (sourceTables.size() - existTables.size()));
		log.info("exist table num : " + existTables.size());
		log.info("not exist table num : " + (sourceTables.size() - existTables.size()));
		
		List<String> insertTableSqls = parseInsertTableSql(existTables);
		recordInfo.setDoInsertTotalNum(insertTableSqls.size());
		recordInfo.addStatisticsInfo("do insert table total num", insertTableSqls.size());
		log.info("do insert table total num : " + insertTableSqls.size());
		
		if(recordInfo.isInsert() == true){
			beginInsert(insertTableSqls);
		}
		
		printResult();
		
		log.info("AutoInsert end...");
	}
	
	/**
	 * 打印最最终的结果信息
	 */
	private void printResult(){
		log.info("=================== start print statistics info ==========================");
		Iterator<String> it = recordInfo.getStatisticsInfo().keySet().iterator();
		while(it.hasNext()){
			String key = it.next();
			Object value = recordInfo.getStatisticsInfo().get(key);
			log.info(key + " : " + value);
		}
		
		log.info("success num : " + recordInfo.getSuccessNum());
		log.info("fail num : " + recordInfo.getFailNum());
		log.info("=================== end print statistics info ==========================");
		
		log.info("=================== begin print fail info ==========================");
		log.info("fail message num : " + recordInfo.getFailMessageList().size());
		for(String mes : recordInfo.getFailMessageList()){
			log.info(mes);
		}
		log.info("=================== end print fail info ==========================");
	}
	
	/**
	 * 对比两个DB中表，解析出sourceTables在targetTables中存在的所有的表
	 * @param sourceTables
	 * @param targetTables
	 * @return
	 */
	private List<String> parseExistTable(List<String> sourceTables, List<String> targetTables){
		List<String> existTables = new ArrayList<String>();
		for(String table : sourceTables){
			if(targetTables.contains(table)){
				existTables.add(table);
			}else{
				log.info("table not exist in targetDB : " + table);
			}
		}
		return existTables;
	}
	
	/**
	 * 从所有存在的表中解析出insert语句
	 * @param existTables
	 * @return
	 */
	private List<String> parseInsertTableSql(List<String> existTables){
		List<String> insertTableSqls = new ArrayList<String>();
		for(String table : existTables){
			if(isDoInsert(table) == false){
				log.info("table not do insert : " + table);
				continue;
			}
			
			String insertSql = null;
			if(table.startsWith("ods_") || table.startsWith("cdr_")){
				insertSql = "insert overwrite table " + recordInfo.getTargetDB() + "." + table + " partition(pt_time_='2013-12-12')";
			}
			else if(table.startsWith("dw_") || table.startsWith("st_")){
				insertSql = "insert overwrite table " + recordInfo.getTargetDB() + "." + table + " partition(pt_time_='2013-12-12', pt_city_='4')";
			}
			else if(table.startsWith("dim_")){
				insertSql = "insert overwrite table " + recordInfo.getTargetDB() + "." + table;
			}
			else{
				log.info("table not do insert : " + table);
				continue;
			}
			
//			sql = "insert overwrite table " + recordInfo.getTargetDB() + "." + table + " select * from " + recordInfo.getSourceDB() + "."+ table;
			
			String selectSql = parseSelect(table);
			String sql = insertSql + " " + selectSql;
			log.info("parse sql : " + sql);
			insertTableSqls.add(sql);
		}
		return insertTableSqls;
	}
	
	private boolean isDoInsert(String tableName){
		for(String table : noInsertTable){
			if(table.equals(tableName)){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * 构造select语句:时间字段根据判断来转换格式
	 * @param tableName
	 * @return
	 */
	private String parseSelect(String tableName){
		String hql = "desc " + recordInfo.getSourceDB() + "." + tableName;
		List<String> columns = HiveConnHelper.getInstance().executeQuery(recordInfo.getSourceDB(), hql, "col_name");
		
		if(!columns.contains("op_time")){//不需要转换时间列值
			return "select * from " + recordInfo.getSourceDB() + "." + tableName;
		}
		if(isNeedChangeTimeFormat(tableName) == false){//不需要转换时间列值
			return "select * from " + recordInfo.getSourceDB() + "." + tableName;
		}
		
		//需要转换时间列值
		StringBuffer colStr = new StringBuffer();
		for(String column : columns){
			if("op_time".equals(column.trim())){
				colStr.append("from_unixtime(unix_timestamp(" + column.trim() + ", 'yyyyMMdd'), 'yyyy-MM-dd')").append(",");
			}else{
				colStr.append(column.trim()).append(",");
			}
		}
		return "select " + colStr.delete(colStr.length() -1, colStr.length()).toString() + " from " + recordInfo.getSourceDB() + "." + tableName;
	}
	/**
	 * 判断时间字段的数据格式是否正确，若不正确则需要进行转换
	 * @param tableName
	 * @return
	 */
	private boolean isNeedChangeTimeFormat(String tableName){
		String hql = "select * from " + recordInfo.getSourceDB() + "." + tableName + " limit 1";
		List<String> list = HiveConnHelper.getInstance().executeQuery(recordInfo.getSourceDB(), hql, "op_time");
		if(list == null || list.size() == 0){
			return false;
		}
		for(String timeValue : list){
			if(timeValue != null && timeValue.length() == 8){//如果时间格式为YYYYMMDD，则需要进行转换
				log.info("need change time format [table : " + tableName + "] [time : " + timeValue + "]");
				return true;
			}
			log.info("no need change time format [table : " + tableName + "] [time : " + timeValue + "]");
			break;
		}
		return false;
	}
	
	/**
	 * 开始并行执行insert
	 * @param insertTableSqls
	 */
	private void beginInsert(List<String> insertTableSqls){
		log.info("begin insert thread pool...");
		
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(this.parallel, this.parallel * 2, 5, 
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),  new ThreadPoolExecutor.CallerRunsPolicy()); 
		 
		for(String sql : insertTableSqls){
			ThreadHiveInsertTask task = new ThreadHiveInsertTask(sql, this);
			threadPool.execute(task);
		}
		
		while(threadPool.getCompletedTaskCount() < insertTableSqls.size()){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		threadPool.shutdown();
		
		log.info("end insert thread pool...");
	}
	
	private static class ThreadHiveInsertTask implements Runnable{
		AutoInsert autoInsert;
		String hql;
		
		public ThreadHiveInsertTask(String hql, AutoInsert autoInsert){
			this.hql = hql;
			this.autoInsert = autoInsert;
		}
		
		@Override
		public void run() {
			log.info("begin run insert thread task...");
			ResultData resultData;
			if(autoInsert.getConfig() == null){
				resultData = HiveConnHelper.getInstance().executeHQL(hql);	
			}
			else{
				resultData = HiveConnHelper.getInstance().executeHQL(new String[]{"set " + autoInsert.getConfig(), hql});
			}
			
			log.info("execute hive insert result : " + resultData.isResult() + " [" + hql + "]");
			autoInsert.record(resultData);
			log.info("end run insert thread task...");
		}
	}
	
	private void record(ResultData resultData){
		synchronized(recordInfo){
			if(resultData.isResult() == true){
				recordInfo.addSuccessNum();
			}
			else{
				recordInfo.addFailNum();
				if(resultData.getMessage() != null){
					recordInfo.addFailMessage(resultData.getMessage());
				}
			}
			log.info("do insert progress : " + recordInfo.getProgressInfo());
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AutoInsert autoInsert = new AutoInsert();
		boolean res = autoInsert.init(args);
		if(res == false){
			return;
		}
		autoInsert.start();	
	}
}