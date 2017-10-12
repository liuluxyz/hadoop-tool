package listFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListFiles {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
//			System.setProperty("HADOOP_USER_NAME", "hadoop");
			Configuration conf = new Configuration();
			conf.addResource(new Path("/hadoop_tool/src/listFiles/core-site.xml"));
			conf.addResource(new Path("/hadoop_tool/src/listFiles/hdfs-site.xml"));
//			conf.addResource(new FileInputStream(new File("D:\\workspace\\workspace_tools\\hadoop_tool\\src\\listFiles\\core-site.xml")));
//			conf.addResource("D:\\workspace\\workspace_tools\\hadoop_tool\\src\\listFiles\\core-site.xml");
//			conf.set("fs.defaultFS", "hdfs://10.1.253.153:8130");
//			conf.set("fs.defaultFS", "hdfs://10.1.253.176:8020");
			conf.set("fs.defaultFS", "hdfs://cdh5cluster");
			conf.set("dfs.nameservices", "cdh5cluster");
			conf.set("dfs.ha.namenodes.cdh5cluster", "nn1,nn2");
			conf.set("dfs.namenode.rpc-address.cdh5cluster.nn1", "10.1.253.153:8130");
			conf.set("dfs.namenode.rpc-address.cdh5cluster.nn2", "10.1.253.154:8130");
			
			System.out.println(conf.get("fs.defaultFS"));
			
			FileSystem fs = FileSystem.get(conf);
//			System.out.println(fs.getName());
//			System.out.println(fs.getScheme());
//			System.out.println(fs.getHomeDirectory());
			boolean a = fs.mkdirs(new Path("/liulu"));
			System.out.println(a);
			
//			FileStatus[] files = fs.listStatus(new Path("/liulu"));
//			for(FileStatus file : files){
//				System.out.println(file.getPath().getName());
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
