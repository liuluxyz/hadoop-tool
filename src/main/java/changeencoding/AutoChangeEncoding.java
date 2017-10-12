package changeencoding;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;

import java.io.Writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import common.EncodeDetector;


/**
 * 不完善，不能使用
 * 可以使用linux命令：iconv 
 * liulu5 2013-12-16
 */
public class AutoChangeEncoding {

	private static final Log log = LogFactory.getLog(AutoChangeEncoding.class);
	
	FileSystem fs;
	int bufferSize = 4096;
	String sourceFile;
	String targetFile;

	
	boolean isLocal = false;
	String inFile;
	String outFile;
	String inEncoding;
	String outEncoding;
	
	public AutoChangeEncoding() {
		if(this.isLocal == false){
			try {
				fs = FileSystem.get(new Configuration());
			} catch (IOException e) {
				e.printStackTrace();
			}	
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
			if("-infile".equals(args[i])){
				this.inFile = args[i+1];
				i++;
			}
			else if("-outfile".endsWith(args[i])){
				this.outFile = args[i+1];
				i++;
			}
			else if("-inencoding".endsWith(args[i])){
				this.inEncoding = args[i+1];
				i++;
			}
			else if("-outencoding".endsWith(args[i])){
				this.outEncoding = args[i+1];
				i++;
			}
			else if("-local".endsWith(args[i])){
				this.isLocal = true;
			}
			else{
				log.info("参数不支持 : " + args[i]);
				this.printUsage();
				return false;
			}
		}
		
		if(inFile == null || outFile == null){
			log.info("指定输入和输出文件");
			this.printUsage();
			return false;
		}
		return true;
	}
	
	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("选项包括:");
		log.info("	-h	" 			+ "	显示帮助信息");
		log.info("	-infile	"		+ "	指定输入文件");
		log.info("	-outfile"		+ "	指定输出文件");
		log.info("	-inencoding"	+ "	指定输入文件的编码");
		log.info("	-outencoding"	+ "	指定输出文件的编码");
	}
	
	private void change() {

	}



	/**
	 * 1. 批量将目录下的数据文件进行格式转换 2. 指定hadoop上的目录文件，进行格式转换
	 */

//	public void start() {
//		// GBK编码格式源码路径
//		String srcDirPath = "D:\\dev\\workspace\\masdev\\mas\\src";
//		// 转为UTF-8编码格式源码路径
//		String utf8DirPath = "D:\\UTF8\\src";
//
//		// 获取所有java文件
//		Collection<File> javaGbkFileCol = FileUtils.listFiles(new File(
//				srcDirPath), new String[] { "java" }, true);
//
//		for (File javaGbkFile : javaGbkFileCol) {
//			// UTF8格式文件路径
//			String utf8FilePath = utf8DirPath
//					+ javaGbkFile.getAbsolutePath().substring(
//							srcDirPath.length());
//			// 使用GBK读取数据，然后用UTF-8写入数据
//			try {
//				FileUtils.writeLines(new File(utf8FilePath), "UTF-8",
//						FileUtils.readLines(javaGbkFile, "GBK"));
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}

	public void start(){
		log.info("AutoChangeEncoding start...");
		try {
			if(this.isLocal){
				convert(this.inFile, this.outFile, this.inEncoding, this.outEncoding);
			}
			else{
				convertHadoopFile(this.inFile, this.outFile, this.inEncoding, this.outEncoding);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("AutoChangeEncoding end...");
	}
	
	private void convertHadoopFile(String infile, String outfile, String from, String to) throws Exception{
		log.info("begin convertHadoopFile...");
		
		if (from == null)
			from = getEncoding(infile);
		if (to == null)
			to = System.getProperty("file.encoding");
		
		FSDataInputStream in = null;
		FSDataOutputStream out = null;

		byte[] buffer = new byte[bufferSize];
		try {
			in = fs.open(new Path(infile));
			out = fs.create(new Path(outfile), false, bufferSize);

			while (true) {
				int curSize = in.read(buffer, 0, bufferSize);
				if (curSize < 0) {
					break;
				}

				byte[] writeBuffer = new String(buffer).getBytes(to);
				out.write(writeBuffer, 0, writeBuffer.length);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		log.info("end convertHadoopFile...");
	}
	
	private void convert(String infile, String outfile, String from, String to) 
			throws Exception {

		InputStream in;
		if (infile != null)
			in = new FileInputStream(infile);
		else
			in = System.in;

		OutputStream out;
		if (outfile != null)
			out = new FileOutputStream(outfile);
		else
			out = System.out;

		if (from == null)
			from = getEncoding(infile);
		if (to == null)
			to = System.getProperty("file.encoding");
		
		Reader r = new BufferedReader(new InputStreamReader(in, from));
		Writer w = new BufferedWriter(new OutputStreamWriter(out, to));

		char[] buffer = new char[4096];
		int len;
		while ((len = r.read(buffer)) != -1)
			w.write(buffer, 0, len);

		r.close();
		w.flush();
		w.close();
	}


	private String getEncoding(String file) throws Exception{
		if(this.isLocal == false){
			FSDataInputStream in = fs.open(new Path(file));
			BufferedInputStream stream = new BufferedInputStream(in);
			return EncodeDetector.getEncoding(stream);
		}
		else{
			InputStream in = new FileInputStream(file);
			BufferedInputStream stream = new BufferedInputStream(in);
			return EncodeDetector.getEncoding(stream);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		AutoChangeEncoding change = new AutoChangeEncoding();
		boolean res = change.init(args);
		if(res == false){
			return;
		}
		
		change.start();

	}

}
