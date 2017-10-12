package loadData;

import hivetool.HiveConnHelper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import common.ResultData;

/**
 * 将hadoop目录下的文件导入库中对应的表,表名根据文件进行解析.含有特殊导入规则，只用于山东经分云化
 * liulu5
 * 2013-12-13
 */
public class Load {

	private static final Log log = LogFactory.getLog(Load.class);
	
	private String path;
	String db;
	boolean isLoad = false;//是否load，若为false，则只做check
	
	FileSystem fs;
	
	public Load(){
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
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
			if("-load".equals(args[i])){
				isLoad =  true;
			}
			else if("-path".equals(args[i])){
				path = args[i+1];
				i++;
			}
			else if("-db".equals(args[i])){
				db = args[i+1];
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
		if(path == null){
			log.info("请输入文件路径");
			this.printUsage();
			return false;
		}
		
		log.info("isLoad : " + isLoad);
		log.info("path : " + path);
		log.info("db : " + db);
		return true;
	}
	
	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("说明: 将hadoop目录下的文件导入库中对应的表,表名根据文件进行解析，含有特殊导入规则，只用于山东经分云化");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-load	"		+ "	是否执行load to hive操作，若不指定，则只做check");
		log.info("	-path"			+ "	指定数据目录");
		log.info("	-db"			+ "	指定导入DB");
	}
	
	public void start() throws IOException{
		log.info("start load...");
		
		ArrayList<FileTable> al = getFileList();
		
		checkTableExist(al);
		printNoExistTable(al);
		
		Map<String, List<String>> load = parseLoadFileTable(al);
		printLoadTableInfo(load);
		
		if(isLoad == true){
			startToLoad(load);	
		}
		
		log.info("end load...");
	}
	
	/**
	 * 打印需要导入但不存在的表
	 * @param al
	 */
	private void printNoExistTable(ArrayList<FileTable> al){
		log.info("begin to print no exist table...");
		List<String> noExistTable = new ArrayList<String>();
		for(FileTable fileTable : al){
			if(!fileTable.isTableExist()){
				if(!noExistTable.contains(fileTable.getTablename())){
					log.info(fileTable.getTablename() + " : " + fileTable.getFilename());
					noExistTable.add(fileTable.getTablename());
				}
			}
		}
		log.info("total no exist table num : " + noExistTable.size());
		log.info("end to print no exist table...");
	}
	
	/**
	 * 打印load的信息：包括表和文件
	 * @param load
	 */
	private void printLoadTableInfo(Map<String, List<String>> load){
		log.info("begin to print load table info...");
		log.info("need load table size : " + load.keySet().size());
		Iterator<String> it = load.keySet().iterator();
		while(it.hasNext()){
			String tablename = it.next();
			List<String> filenames = load.get(tablename);
			log.info(tablename + " : " + filenames.size());
			for(String str : filenames){
				log.info("        " + str);
			}
			log.info("-----------");
		}
		log.info("end to print load table info...");
	}
	
	/**
	 * 获取file列表
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private ArrayList<FileTable> getFileList() throws FileNotFoundException, IOException{
		FileStatus[] files = fs.listStatus(new Path(path));
		ArrayList<FileTable> al = new ArrayList<FileTable>();
		log.info("get file num : " + files.length);
		for(FileStatus file : files){
			String filename = file.getPath().getName();
			boolean isLoad = isLoad(filename);
			String tablename = null;
			if(isLoad == true){
				tablename = parseTableName(filename);
			}

			FileTable fileTable = new FileTable(filename, tablename);
			fileTable.setLoad(isLoad);
			
			al.add(fileTable);
		}
		return al;
	}
	
	/**
	 * 执行load to hive
	 * @param load
	 */
	private void startToLoad(Map<String, List<String>> load){
		log.info("start to load...");
		
		int tableTotal = load.size();
		int fileTotal = parseFileTotalNum(load);
		int tableProgress = 0;
		int fileProgress = 0;
		
		Iterator<String> it = load.keySet().iterator();
		while(it.hasNext()){
			tableProgress++;
			String tablename = it.next();
			List<String> files = load.get(tablename);
			
			boolean isFirst = true;
			for(String file : files){
				fileProgress++;
				String loadSql = null;
				if(isFirst == true){
					loadSql = "load data inpath '" + path + "/" + file + "' overwrite into table " + db + "." + tablename;
					isFirst = false;
				}else{
					loadSql = "load data inpath '" + path + "/" + file + "' into table " + db + "." + tablename;
				}
				ResultData res = HiveConnHelper.getInstance().executeHQL(db, loadSql);
				log.info("load result : "+ res.isResult() + " - [" + loadSql + "]");
				log.info("load progress(file) : "+ fileProgress + " of " + fileTotal);
			}
			log.info("load progress(table) : "+ tableProgress + " of " + tableTotal);
		}
		log.info("end to load...");
	}
	
	/**
	 * 计算需要导入的数据总数
	 * @param load
	 * @return
	 */
	private int parseFileTotalNum(Map<String, List<String>> load){
		int total = 0;
		Iterator<List<String>> files = load.values().iterator();
		while(files.hasNext()){
			List<String> file = files.next();
			total += file.size();
		}
		return total;
	}
	
	/**
	 * 检查表是否存在
	 * @param al
	 */
	private void checkTableExist(final ArrayList<FileTable> al){
		List<String> tablenames = HiveConnHelper.getInstance().getAllTables(db);
		for(FileTable filetable : al){
			for(String str : tablenames){
				if(filetable.getTablename() == null){
					filetable.setTableExist(false);
				}
				else if(filetable.getTablename().equals(str)){
					filetable.setTableExist(true);
					break;
				}
			}
		}		
	}
	
	/**
	 * 通过文件名判断此文件是否需要load
	 * @param filename
	 * @return
	 */
	private boolean isLoad(String filename){
		String[] containsNoLoad = new String[]{
				"201306",
				"201307",
				"201211",
				"20131201",
				"20131202",
				"20131203",
				"20131204",
				"20131205",
				"20131206",
				"20131207",
				"20131208",
				"20131209",
				"20131210",
		};
		for(String str : containsNoLoad){
			if(filename.indexOf(str) != -1){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * 从文件名中解析表名
	 * @param fileName
	 * @return
	 */
	public String parseTableName(String fileName){
		String tablename = fileName;
		while(tablename.lastIndexOf(".") > 0){
			tablename = tablename.substring(0, tablename.lastIndexOf("."));
		}
		
		Map<String, String> replace = ReplaceConf.get();
		
		Iterator<String> it = replace.keySet().iterator();
		while(it.hasNext()){
			String key = it.next();
			String value = replace.get(key);
			tablename = tablename.replaceAll(key, value);
		}
		
		if(log.isDebugEnabled()){
			log.debug("parseTableName : [" + fileName + "] to [" + tablename + "]");	
		}
		return tablename;
	}
	
	/**
	 * 从文件列表中解析出需要做load的文件及其表
	 * @param al
	 * @return
	 */
	private Map<String, List<String>> parseLoadFileTable(ArrayList<FileTable> al){
		Map<String, List<String>> load = new HashMap<String, List<String>>();
		for(FileTable filetable : al){
			if(filetable.isLoad() && filetable.isTableExist()){
				if(load.containsKey(filetable.getTablename())){
					load.get(filetable.getTablename()).add(filetable.getFilename());
				}else{
					List<String> list = new ArrayList<String>();
					list.add(filetable.getFilename());
					load.put(filetable.getTablename(), list);
				}
			}
		}
		return load;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Load load = new Load();
		boolean res = load.init(args);
		if(res == false){
			return;
		}
		
		try {
			load.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

