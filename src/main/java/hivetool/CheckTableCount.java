package hivetool;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 查询指定的hive中的所有的表的记录数
 * liulu5
 * 2013-12-16
 */
public class CheckTableCount {

	private static final Log log = LogFactory.getLog(CheckTableCount.class);
	
	String db;
	
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
			}else{
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
		log.info("	-db	"		+ "	指定DB");
	}
	
	public void start() throws IOException{
		log.info("start...");
		
		List<String> tablenames = HiveConnHelper.getInstance().getAllTables(db);
		for(String table : tablenames){
			int count = HiveConnHelper.getInstance().queryTableCountValue(db, table);
		
			log.info("table : " + table + " count : " + count);
		}
		
		log.info("end...");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		CheckTableCount check = new CheckTableCount();
		boolean res = check.init(args);
		if(res == false){
			return;
		}
		
		try {
			check.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

