package generateFiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenerateFiles {

	int num;
	String fileNamePre;
	long totalSize = 100 * 1024;
	int bufferSize = 4096;
	byte[] buffer;
	FileSystem fs;
	String path;
	
	public GenerateFiles(){
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
//		for(int i=0; i < bufferSize; i+=4){
//			buffer[i] = "3".getBytes()[0];
//			buffer[i+1] = ",".getBytes()[0];
//			buffer[i+2] = (byte)('0' + i % 50);
//			buffer[i+3] = (byte)10;
//		}
		
//		String temp = "1	a1	a2	a3\r\n2	b1	b2	b3\r\n3	c1	c2	c3";
		String temp = "1	a1	a2	a3\r\n2	b1	b2	b3\r\n3	c1	c2	c3";
		buffer = temp.getBytes();
		
	}
	
	public void generateFiles() {
		int curNum = 0;
		while(curNum++ < num){
			try {
				generateFile(fileNamePre + curNum);
			} catch (IOException e) {
				e.printStackTrace();
			}
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
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		GenerateFiles gen = new GenerateFiles();
		if(args.length > 0){
			gen.path = args[0];
			gen.num = Integer.parseInt(args[1]);
			gen.fileNamePre = args[2].endsWith("_") ? args[2] : (args[2] + "_");
		}
		gen.generateFiles();
	}

}
