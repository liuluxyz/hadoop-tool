package apendFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AppendFile {

	long totalSize = 100 * 1024;
	int bufferSize = 4096;
	byte[] buffer;
	FileSystem fs;
	String path;
	
	public AppendFile(){
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		buffer = new byte[bufferSize];
//		for(int i=0; i < bufferSize; i+=2){
//			buffer[i] = (byte)('0' + i % 50);
//			buffer[i+1] = (byte)10;
//		}
		for(int i=0; i < bufferSize; i+=4){
			buffer[i] = "3".getBytes()[0];
			buffer[i+1] = ",".getBytes()[0];
			buffer[i+2] = (byte)('0' + i % 50);
			buffer[i+3] = (byte)10;
		}
	}
	
	public void appendFile() throws IOException{
		if(!fs.exists(new Path(path))){
			System.out.println("文件不存在：" + path);
		}
		
		Configuration conf = new Configuration();
		conf.set("dfs.support.append", "true");
		
		FSDataOutputStream out = fs.append(new Path(path), bufferSize);
		
		try{
			long remaining;
			for(remaining = totalSize; remaining > 0; remaining -=bufferSize){
				int curSize = (bufferSize < remaining) ? bufferSize : (int)remaining;
				out.write(buffer, 0, curSize);
			}	
		}finally{
			out.flush();
			out.close();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if(args.length == 0){
			System.out.println("请输入参数!");
			return;
		}
		AppendFile append = new AppendFile();
		
		append.path = args[0];
		try {
			append.appendFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
