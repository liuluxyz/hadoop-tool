package listFiles;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * liulu5
 * 2015-9-24
 */
/**
 * @Description: TODO
 * @author liulu5
 * @date 2015-9-24 下午3:50:07 
 */
public class Test {

	
	private static FileSystem getFS() throws IOException{
		Configuration conf = new Configuration();
//		conf.addResource(new Path("/hadoop_tool/src/listFiles/core-site.xml"));
//		conf.addResource(new Path("/hadoop_tool/src/listFiles/hdfs-site.xml"));
		
//		conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\get\\core-site.xml"));
//		conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\get\\hdfs-site.xml"));
		
//		conf.set("fs.defaultFS", "hdfs://10.1.253.178:9020");
		
		
		conf.set("fs.defaultFS", "hdfs://bjydcluster");
		conf.set("dfs.nameservices", "bjydcluster");
		conf.set("dfs.ha.namenodes.bjydcluster", "nn1,nn2");
		conf.set("dfs.namenode.rpc-address.bjydcluster.nn1", "10.4.56.1:9000");
		conf.set("dfs.namenode.rpc-address.bjydcluster.nn2", "10.4.56.3:9000");
		conf.set("dfs.client.failover.proxy.provider.bjydcluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		
		FileSystem fs = FileSystem.get(conf);
		
//		System.out.println(fs.getName());
//		System.out.println(fs.getScheme());
//		System.out.println(fs.getHomeDirectory());
//		boolean a = fs.mkdirs(new Path("/tmp"));
//		System.out.println(a);
		
		return fs;
	}
	
	private static void test(){
		try {
			FileSystem fs = getFS();
			FileStatus[] files = fs.listStatus(new Path("/liulu"));
			for(FileStatus file : files){
				System.out.println(file.getBlockSize());
				System.out.println(file.isDirectory());
				System.out.println(file.getPath().getName());
				System.out.println(file.getPath().toString());
				System.out.println("------------------------");
			}
		} catch (IOException e) {
//			e.printStackTrace();
		}
	}
	
	private static void testFilter(){
		try {
			FileSystem fs = getFS();
			
			String reg = "101_*.*((\\.CSV)|(\\.CHK))";
			Path pToPath = new Path("/liulu");
			RegexExcludePathFilter filter = new RegexExcludePathFilter(reg);
//			FileStatus[] fileStatusArr = fs.listStatus(pToPath,filter);
//			if(ArrayUtils.isNotEmpty(fileStatusArr)){
//				System.out.println("not empty");
//			}
//			else{
//				System.out.println("empty");
//			}
			
			FileStatus[] files = fs.listStatus(new Path("/liulu"));
			for(FileStatus file : files){
				if (filter.accept(file.getPath())) {
					System.out.println(file.getPath() + " is ok");
				}else{
					System.out.println(file.getPath() + " is not ok");
					System.out.println(file.getPath().getName() + " is not ok");
				}
			}
			
		} catch (IOException e) {
//			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		testFilter();
	}

}

