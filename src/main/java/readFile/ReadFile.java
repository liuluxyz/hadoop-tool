package readFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFile {

	long totalSize = 100 * 1024;
	int bufferSize1 = 4096;
	int bufferSize2 = 8192;
	byte[] buffer;
	FileSystem fs;
	String path;
	
	public ReadFile(){
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void readFile1(){
		FSDataInputStream in = null;
		buffer = new byte[bufferSize1];
		try {
			long startTime = System.currentTimeMillis();
			long actualSize = 0;
			in = fs.open(new Path(path));
			while(actualSize < totalSize){
				int curSize = in.read(buffer, 0, bufferSize1);
				if(curSize < 0){
					break;	
				}
				actualSize += curSize;
			}
			long endTime = System.currentTimeMillis();
			long execTime = endTime - startTime;
			System.out.println("readFile1 : read size : " + actualSize + ", execute time : " + execTime + "ms");
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void readFile2(){
		FSDataInputStream in = null;
		buffer = new byte[bufferSize2];
		try {
			long startTime = System.currentTimeMillis();
			long actualSize = 0;
			in = fs.open(new Path(path));
			while(actualSize < totalSize){
				int curSize = in.read(buffer, 0, bufferSize2);
				if(curSize < 0){
					break;	
				}
				actualSize += curSize;
			}
			long endTime = System.currentTimeMillis();
			long execTime = endTime - startTime;
			System.out.println("readFile2 : read size : " + actualSize + ", execute time : " + execTime + "ms");
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void readFileContent(){
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(path));
			
			int num = 0;
			String line = in.readLine();
			while(line != null){
				System.out.println(line);
				line = in.readLine();
				num++;
			}
			System.out.println(num);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ReadFile read = new ReadFile();
		read.path = args[0];
//		read.readFile1();
//		read.readFile2();
		read.readFileContent();
	}

}
