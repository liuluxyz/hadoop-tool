package copyfile;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.DistCp;

/**
 * 从源目录向目标目录拷贝文件
 * liulu5
 * 2013-12-13
 */
public class CopyFile {

	private static final Log log = LogFactory.getLog(CopyFile.class);
	
	private String sourcePath = "/data/20131211";
	private String targetPath = "/data/load20131211";
	
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
			if("-sourcepath".equals(args[i])){
				sourcePath = args[i+1];
				i++;
			}
			else if("-targetpath".endsWith(args[i])){
				targetPath = args[i+1];
				i++;
			}
			else{
				log.info("参数不支持 : " + args[i]);
				this.printUsage();
				return false;
			}
		}
		
		log.info("sourcePath : " + sourcePath);
		log.info("targetPath : " + targetPath);
		return true;
	}
	
	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("说明: 从源目录向目标目录拷贝文件");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-sourcepath	"		+ "	指定源目录");
		log.info("	-targetpath"	+ "	指定目标目录");
	}
	
	public void copy() throws IOException{
		log.info("start copy...");
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] files = fs.listStatus(new Path(sourcePath));
		log.info("start copy file num : " + files.length);
		int num = 0;
		for(FileStatus file : files){
			String filename = file.getPath().getName();
			log.info("copying : " + filename);
			FSDataInputStream in = fs.open(new Path(sourcePath + "/" + filename));
			FSDataOutputStream out = fs.create(new Path(targetPath + "/" + filename));
			IOUtils.copyBytes(in, out, conf);
			in.close();
			out.close();
			
			num++;
			log.info("copy file success : " + filename);
			log.info("copy progress : "+ num + " of " + files.length);
			
		}
		log.info("end copy file total num : " + num);
		log.info("end copy...");
	}
	
//	private void distcp() throws IOException{
//		 DistCp.copy(new JobConf(new Configuration()), "", "", null, true, false);
//		 
//	}
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		CopyFile copy = new CopyFile();
		boolean res = copy.init(args);
		if(res == false){
			return;
		}
		
		try {
			copy.copy();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

