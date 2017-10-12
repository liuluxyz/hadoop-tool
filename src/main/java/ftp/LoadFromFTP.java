package ftp;

import hivetool.HiveConnHelper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import loadData.Load;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import common.ResultData;

/**
 * 自动导数据
 * 使用方法：
	1.登录机器：OCDC-DD-001
	2.进入目录：/home/ocdc/liulu/tool/bin/
	3.执行shell脚本：./loadFromFTP.sh -table xxx
	4.查看导数据的过程和结果的日志：tail -f /home/ocdc/liulu/tool/logs/loadFromFTP.log

 * 使用工具的约定规则：
	1. 此工具必须保证张超已经把数据放在ftp上之后，才能执行。
	2. 执行上述导入脚本，必须且仅需要指定表名。
	3. 默认从张超导数据的ftp服务器获取数据。
	4. 默认往hive的db4中导数据。
	5. 此数据的导入方式为覆盖。
	6. 导数据不指定分区等信息。
	7. 从ftp获取的数据文件会在指定目录(/mnt/nas/data)进行保存。
	8. 导数据的结果，请在指定日志中查看。
	9. 一次执行可以指定多个表。
 * @author liulu5
 *
 */
public class LoadFromFTP {

	private static final Log log = LogFactory.getLog(LoadFromFTP.class);
	
	private final String ftpIP = "172.17.254.10";
	private final String username = "dsadm";
	private final String password = "dsadmsd1";
	private final String path = "/bassgpfs2/export_load/del";
	
	private final String localpath = "/mnt/nas/data";
	
	private final String db = "db4";
	
	private String[] tableNames;
	private List<String> fileNameList = new ArrayList<String>();
	
	private Load loadHelper = new Load();
	
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
			else if("-table".equals(args[i])){
				if(args[i+1] != null){
					tableNames = args[i+1].split(";");
				}
				i++;
			}
			else{
				log.info("参数不支持 : " + args[i]);
				this.printUsage();
				return false;
			}
		}
		
		if(tableNames == null || tableNames.length == 0){
			log.info("请输入表名");
			this.printUsage();
			return false;
		}
		log.info("tableName : " + Arrays.toString(tableNames));
		return true;
	}
	
	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("说明: 从ftp获取数据导入到hive的DB4库中，含有特殊导入规则，只用于山东经分云化");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-table"			+ "	指定表名,多个表使用分号分隔");
	}
	
	public void start(){
		for(int i=0; i<tableNames.length; i++){
			log.info("进度: 开始第" + (i+1) + "个,表" + tableNames[i] + ",总个数" + tableNames.length);
//			try{
				boolean result = downFile(tableNames[i]);
				if(result == false){
					return;
				}
				
				startToLoad(tableNames[i]);
//			}finally{
//				deleteData();
//			}
			log.info("进度: 结束第" + (i+1) + "个,表" + tableNames[i] + ",总个数" + tableNames.length);
		}
	}
	
	private void deleteData(){
		for(String fileName : fileNameList){
			File file = new File(localpath + "/" + fileName);
			if(file.exists()){
				file.delete();
			}
		}
	}
	
	/**
	 * 执行load
	 */
	private void startToLoad(String tableName){
		log.info("start to load...[" + tableName + "]");
		
		int fileProgress = 0;
		boolean isFirst = true;
		for(String fileName : fileNameList){
			fileProgress++;
			String loadSql = null;
			if(isFirst == true){
				loadSql = "load data local inpath '" + localpath + "/" + fileName + "' overwrite into table " + db + "." + tableName;
				isFirst = false;
			}else{
				loadSql = "load data local inpath '" + localpath + "/" + fileName + "' into table " + db + "." + tableName;
			}
			ResultData res = HiveConnHelper.getInstance().executeHQL(db, loadSql);
			if(res.isResult() == false){
				log.info("执行load失败！");
				return;
			}
			log.info("load文件结果 : "+ res.isResult() + " - [" + loadSql + "]");
			log.info("load文件进度(file) : [" + tableName + "]" + fileProgress + " of " + fileNameList.size());
		}
		
		log.info("end to load...[" + tableName + "]");
	}
	
	/**
	 * 从ftp获取数据
	 * @return
	 */
	private boolean downFile(String tableName) {
		fileNameList.clear();
	    FTPClient ftp = new FTPClient();
	    try {
	        int reply;
	        ftp.connect(ftpIP);
	        ftp.login(username, password);//登录 
	        reply = ftp.getReplyCode();
	        if (!FTPReply.isPositiveCompletion(reply)) {
	            ftp.disconnect();
	            log.info("连接ftp失败");
	            return false;
	        }
	        ftp.changeWorkingDirectory(path);//转移到FTP服务器目录
	        FTPFile[] fs = ftp.listFiles();
	        for(FTPFile ff:fs){
	        	String fileName = ff.getName();
//	        	log.info("file name : " + fileName);
	        	if(fileName == null){
	        		continue;
	        	}
	        	String tableNameFromFile;
	        	if(fileName.startsWith("dmrn")){
	        		String temp = fileName.replaceAll("201312_", "");
	        		tableNameFromFile = loadHelper.parseTableName(temp);
	        	}else{
	        		tableNameFromFile = loadHelper.parseTableName(fileName);
	        	}
	        	
	            if(tableNameFromFile.equals(tableName)){
	            	log.info("开始获取ftp文件 : " + fileName);
	                File localFile = new File(localpath+"/" + fileName);
	                if(localFile.exists()){
	                	localFile.delete();
	                	localFile.createNewFile();
	                }
	                OutputStream is = new FileOutputStream(localFile);
	                ftp.retrieveFile(ff.getName(), is);
	                is.close();
	                fileNameList.add(fileName);
	                log.info("结束获取ftp文件 : " + fileName);
	            }
	        }
	        log.info("表[" + tableName + "]获取文件个数：" + fileNameList.size());
	        ftp.logout();
	        if(fileNameList.size() > 0){
	        	return true;
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	        log.error("从ftp获取数据失败：" + e.toString(), e);
	        return false;
	    } finally {
	        if (ftp.isConnected()) {
	            try {
	                ftp.disconnect();
	            } catch (IOException ioe) {
	            }
	        }
	    }
	    return false;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		LoadFromFTP loadFtp = new LoadFromFTP();
		if(loadFtp.init(args) == false){
			return;
		}
		loadFtp.start();
	}

}
