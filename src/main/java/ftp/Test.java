package ftp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import loadData.Load;

/**
 * liulu5
 * 2013-12-26
 */
public class Test {

	public static void test(){
		String reg = "category_20140806.dat";
	    FTPClient ftp = new FTPClient();
	    try {
	        ftp.connect("203.195.197.27");
	        ftp.login("gather", "gather");//登录 
	        int reply = ftp.getReplyCode();
	        if (!FTPReply.isPositiveCompletion(reply)) {
	            ftp.disconnect();
	        }
//	        ftp.changeWorkingDirectory("/gather");//转移到FTP服务器目录
	        FTPFile[] fs = ftp.listFiles("/gather/category_20140806.dat");
	        FTPFile file = null;
	        for(FTPFile ff:fs){
	    			//对老配置进行兼容（老配置过滤条件为reg:开头）
	    			Pattern p = Pattern.compile(reg.replace("reg:", ""));
	    			System.out.println(p);
	    			Matcher m = p.matcher(ff.getName());
	    			System.out.println("test FTPFile get name : " + ff.getName());
	    			System.out.println(m.matches());
//	    			return m.matches();
	    			if(m.matches()){
	    				file = ff;
	    			}
	    		System.out.println("fs : " + fs[0]);
	        }
	        
//	        File localFile = new File("D:\\workspace\\workspace_tools\\hadoop_tool\\src\\ftp\\file");
//            if(localFile.exists()){
//            	localFile.delete();
//            	localFile.createNewFile();
//            }
//            System.out.println("aaa  : " + file.getName());
//            OutputStream is = new FileOutputStream(localFile);
//            ftp.retrieveFile(file.getName(), is);
//            is.close();
	        
	    } catch (IOException e) {
	        e.printStackTrace();
	    } finally {
	        if (ftp.isConnected()) {
	            try {
	                ftp.disconnect();
	            } catch (IOException ioe) {
	            }
	        }
	    }

	}
	
	public static void test2(){
		String filePath = "/home/ocetl/data_zhanghg/yst/category_20140806.dat";
	    FTPClient ftp = new FTPClient();
	    try {
	        ftp.connect("172.16.2.103");
	        ftp.login("ocetl", "ocetl");//登录 
	        int reply = ftp.getReplyCode();
	        if (!FTPReply.isPositiveCompletion(reply)) {
	            ftp.disconnect();
	        }

	        FTPFile[] fs = ftp.listFiles(filePath);
	        FTPFile file = null;
	        for(FTPFile ff:fs){
	    			System.out.println("test2 FTPFile get name : " + ff.getName());
//	    			if(m.matches()){
//	    				file = ff;
//	    			}
	    		System.out.println("fs : " + fs[0]);
	        }
	        
//	        File localFile = new File("D:\\workspace\\workspace_tools\\hadoop_tool\\src\\ftp\\file");
//            if(localFile.exists()){
//            	localFile.delete();
//            	localFile.createNewFile();
//            }
//            System.out.println("aaa  : " + file.getName());
//            OutputStream is = new FileOutputStream(localFile);
//            ftp.retrieveFile(file.getName(), is);
//            is.close();
	        
	    } catch (IOException e) {
	        e.printStackTrace();
	    } finally {
	        if (ftp.isConnected()) {
	            try {
	                ftp.disconnect();
	            } catch (IOException ioe) {
	            }
	        }
	    }

	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
         
		test();
		test2();
		
		/**
		try {
			 File localFile = new File(args[0]);
	         if(localFile.exists()){
	         	boolean res = localFile.delete();
	         	System.out.println("delete result : " + res);
	         	
	         	 res = localFile.createNewFile();
		         	System.out.println("create new file : " + res);
	         }else{
	        	 System.out.println("file not exist");
	         }
	         
	        
			OutputStream is = new FileOutputStream(localFile);
			is.write("abcdefg".getBytes());
			is.close();
			System.out.println("file write end");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         */
	}

}

