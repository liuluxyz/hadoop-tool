package generateFiles;

import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * generate file for hive table
 * @author liulu5
 * 2013/11/21
 */
public class GenerateHiveFile {
	
	private static final Log log = LogFactory.getLog(GenerateHiveFile.class);
	
	int bufferSize = 1024 * 1024;
	byte[] buffer;
	FileSystem fs;
	
	String warehouse = "/user/hive/warehouse";
	int fileNum = 1;
	long fileSize = 1024;//KB
	String tableName = "temp";
	int columnNum = 1;
	String[] columnType = new String[]{"string"};
	String fileNamePrefix;
	String fieldsTerminated = ",";
	
	private enum Para {warehouse, filenum, filesize, tablename, columnnum, columntype, filenameprefix, fieldsterminated};
	
	public void printUsage(){
		log.info("usage: hadoop jar xxx.jar GenerateHiveFile [-option] [args]");
		log.info("	e.g. hadoop jar tool.jar generatehivefile -filenum 10000 -tablename a -columnnum 2 -columntype int,string");
		log.info("where options incluse:");
		
		log.info("	-" + Para.warehouse 		+ "	define hive warehouse path");
		log.info(							"			use '/user/hive/warehouse' as default");
		
		log.info("	-" + Para.filenum 			+ "	define how many files will be generated");
		log.info(							"			use 1' as default");
		
		log.info("	-" + Para.filesize 			+ "	define single file size(KB) will be generated");
		log.info(							"			use 1024 as default");
		
		log.info("	-" + Para.tablename 		+ "	define which table name the file is in");
		log.info(							"			use 'temp' as default");
		
		log.info("	-" + Para.columnnum 		+ "	define how many columns in the table");
		log.info(							"			use 1 as default");
		
		log.info("	-" + Para.columntype 		+ "	define column type");
		log.info(							"			input one type means all columns use this type; input multi-type, should input correct num [columnnum] type, use comma to split multi-type");
		log.info(							"			support column type: int,bigint,boolean,float,double,string,date");
		log.info(							"			use string as default");
		
		log.info("	-" + Para.filenameprefix 	+ "	define file name prefix");
		log.info(							"			use [tablename] as default");
		
		log.info("	-" + Para.fieldsterminated 	+ "	define fields terminated");
		log.info(							"			use comma as default");
	}
	
	public GenerateHiveFile(){
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void reBuildBuffer(long limitBufferSize){
		
		StringBuffer line = new StringBuffer("");
		while(true){
			for(int i=0; i<columnNum; i++){
				String colType = "String";
				if(columnType.length == 1){
					colType = columnType[0].toLowerCase();
				}else{
					colType = columnType[i].toLowerCase();
				}
				
				if("int".equals(colType)){
					line.append(new Random().nextInt());
				}
				else if("bigint".equals(colType)){
					line.append(new Random().nextLong());
				}
				else if("float".equals(colType)){
					line.append(new Random().nextFloat());
				}
				else if("double".equals(colType)){
					line.append(new Random().nextDouble());
				}
				else if("boolean".equals(colType)){
					line.append((new Random().nextInt() % 2 == 0) ? "true" : "false");
				}
				else if("string".equals(colType)){
//					line.append("temp");
					line.append("temp_" + new Random().nextInt());
				}
				else if("date".equals(colType)){					
					Calendar calendar = Calendar.getInstance();
					int ran = new Random().nextInt(10);
					calendar.add(Calendar.DAY_OF_MONTH, ran);
					String date = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime());
					line.append(date);
				}
				if(i < columnNum-1){
					line.append(fieldsTerminated);
				}
			}
			line.append("\r\n");
			if(line.toString().getBytes().length >= limitBufferSize){
				break;
			}
		}
		
//		int num = 10;
//		while(num-- > 0){//构造数据倾斜
//			line.append("temp").append(fieldsTerminated).append("0").append("\r\n");
//		}
		
		if(line.toString().getBytes().length > limitBufferSize){
			if(line.indexOf("\r\n") != line.lastIndexOf("\r\n")){
				buffer = line.append("\r\n").substring(0, line.lastIndexOf("\r\n")).getBytes();
			}
		}
		else{
			buffer = line.toString().getBytes();
		}
	}
	
	/**
	 * init parameter
	 * @param args
	 * @return
	 */
	public boolean init(String[] args){
		if(args.length == 0){
			log.info("error: please input parameters!");
			return false;
		}
		
		if((args.length % 2) != 0){
			log.info("error: the num of parameters numst be in pairs!");
			return false;
		}
		
		int num = 0;
		while(num < args.length){
			try{
				if(("-" + Para.warehouse).equals(args[num])){
					this.warehouse = args[num + 1];
				}
				else if(("-" + Para.filenum).equals(args[num])){
					this.fileNum = Integer.parseInt(args[num + 1]);
				}
				else if(("-" + Para.filesize).equals(args[num])){
					this.fileSize = Long.parseLong(args[num + 1]);
				}
				else if(("-" + Para.tablename).equals(args[num])){
					this.tableName = args[num + 1];
				}
				else if(("-" + Para.columnnum).equals(args[num])){
					this.columnNum = Integer.parseInt(args[num + 1]);
				}
				else if(("-" + Para.columntype).equals(args[num])){
					this.columnType = args[num + 1].split(",");
				}
				else if(("-" + Para.filenameprefix).equals(args[num])){
					this.fileNamePrefix = args[num + 1];
				}
				else if(("-" + Para.fieldsterminated).equals(args[num])){
					this.fieldsTerminated = args[num + 1];
				}
				else{
					log.info("error: not support parameter : " + args[num]);
					return false;
				}
				num += 2;
			}catch(NumberFormatException e){
				log.error("error: " + args[num] + " should be number!", e);
				return false;
			}
		}
		if(this.columnType != null && this.columnType.length > 1 && this.columnType.length != this.columnNum){
			log.info("error: input multi-type, should input correct num [columnnum] type, columnNum is " + this.columnNum);
			return false;
		}
		return true;
	}
	
	public void generateFiles() {
		log.info("start generate...");
		int curNum = 0;
		while(curNum++ < fileNum){
			try {
				generateFile((fileNamePrefix == null?tableName:fileNamePrefix)  + "-" + curNum);
			} catch (IOException e) {
				log.error("generate Files error", e);
			}
			log.info("generate progress : " + curNum + " of " + fileNum);
		}
		log.info("end generate...");
	}
	
	public void generateFile(String fileName) throws IOException{
		log.info("start generate file : " + fileName);
//		String filepath = warehouse + "/" + tableName + "/" + fileName;
//		FSDataOutputStream out = fs.create(new Path(filepath), true, bufferSize);
		FSDataOutputStream out = fs.create(new Path(warehouse + "/" + tableName, fileName), true, bufferSize);
		try{
			long remaining;
			for(remaining = fileSize * 1024; remaining > 0;){
				reBuildBuffer(remaining > this.bufferSize ? this.bufferSize : remaining);
				out.write(buffer);
				remaining -=buffer.length;
			}
		}finally{
			out.close();
		}
		log.info("end generate file : " + fileName);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GenerateHiveFile gen = new GenerateHiveFile();
		boolean initRes = gen.init(args);
		if(initRes == false){
			gen.printUsage();
			return;
		}
		gen.generateFiles();
	}

}
