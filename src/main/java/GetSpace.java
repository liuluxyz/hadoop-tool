import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class GetSpace {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
//		conf.addResource("D:\\project\\项目现场\\中国移动\\core-site.xml");
//		conf.addResource("D:\\project\\项目现场\\中国移动\\hdfs-site.xml");
		
//		String [][] paras = new String[][];
		
		String nn1 = conf.get("fs.defaultFS");
		System.out.println("fs.defaultFS : " + nn1);
		
		try {
			DFSClient dfs = new DFSClient(conf);
			
			FsStatus fs = dfs.getDiskStatus();
			System.out.println("fs.getCapacity() : " + fs.getCapacity());
			System.out.println("fs.getRemaining() : " + fs.getRemaining());
			System.out.println("fs.getUsed() : " + fs.getUsed());
			
			System.out.println("hive size : " + dfs.getFileInfo("/user/hive").getBlockSize());
			dfs.getNamenode();
			
			DistributedFileSystem ftp = (DistributedFileSystem) FileSystem.get(conf);
			System.out.println("user getBlockSize : " + ftp.getBlockSize(new Path("/911/file999")));
			ftp.getHomeDirectory();
			System.out.println("ftp.getLength : " + ftp.getLength(new Path("/911/file999")));
			
			FileStatus status = ftp.getFileStatus(new Path("/911/file999"));
			System.out.println("status.getBlockSize() : " + status.getBlockSize());
			System.out.println("status.getLen() : " + status.getLen());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
