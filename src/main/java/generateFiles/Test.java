package generateFiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test {

	int num;
	String fileNamePre;
	long totalSize = 100 * 1024;
	int bufferSize = 4096;
	byte[] buffer;
	FileSystem fs;
	String path;
	
	public Test(){
		try {
//			Configuration conf = new Configuration();
//			conf.addResource("D:\\workspace\\workspace_tools\\hadoop_tool\\src\\generateFiles\\core-site-spark.xml");
//			conf.addResource("D:\\workspace\\workspace_tools\\hadoop_tool\\src\\generateFiles\\hdfs-site-spark.xml");
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://sparkstreaming");
			   conf.set("dfs.nameservices", "sparkstreaming");
			   conf.set("dfs.ha.namenodes.sparkstreaming", "nn1,nn2");
			   conf.set("dfs.namenode.rpc-address.sparkstreaming.nn1", "streaming01:8030");
			   conf.set("dfs.namenode.rpc-address.sparkstreaming.nn2", "streaming03:8030");
			   conf.set("dfs.client.failover.proxy.provider.sparkstreaming", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
			   conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			   
			fs = FileSystem.get(conf);
			
//			System.out.println(fs.getConf().get("fs.defaultFS"));
//			System.out.println(fs.getConf().get("dfs.nameservices"));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		buffer = new byte[bufferSize];
		for(int i=0; i < bufferSize; i+=2){
			buffer[i] = (byte)('0' + i % 50);
			buffer[i+1] = (byte)10;
		}
//		for(int i=0; i < bufferSize; i+=4){
//			buffer[i] = "3".getBytes()[0];
//			buffer[i+1] = ",".getBytes()[0];
//			buffer[i+2] = (byte)('0' + i % 50);
//			buffer[i+3] = (byte)10;
//		}
		
//		String temp = "1	a1	a2	a3\r\n2	b1	b2	b3\r\n3	c1	c2	c3";
//		String temp = "1	a1	a2	a3\r\n2	b1	b2	b3\r\n3	c1	c2	c3";
//		buffer = temp.getBytes();
		
	}
	
	public void mkdir() {
		try {
			
			boolean res = fs.mkdirs(new Path("/liulu/b"));
			System.out.println(res);
			
			
			HDFSUtil.createDirectory(fs.getConf(), "/liulu/d");
			
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void generateFiles() {
		int curNum = 0;
		while(curNum++ < num){
			
			final int n = curNum;
			Thread a = new Thread(){
				public void run(){
					try {
						generateFile(fileNamePre + n);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			};
			a.start();
		}
	}
	
	public void generateFile(String fileName) throws IOException{
		FSDataOutputStream out = fs.create(new Path(path, fileName), true, bufferSize);
		try{
			long remaining;
			for(remaining = totalSize; remaining > 0; remaining -=bufferSize){
				int curSize = (bufferSize < remaining) ? bufferSize : (int)remaining;
				out.write(buffer, 0, curSize);
			}
		}finally{
			out.close();
			fs.close();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Test gen = new Test();
//		gen.mkdir();
		
		
		if(args.length > 0){
			gen.path = args[0];
			gen.num = Integer.parseInt(args[1]);
			gen.fileNamePre = args[2].endsWith("_") ? args[2] : (args[2] + "_");
		}
		gen.generateFiles();
	}

}
