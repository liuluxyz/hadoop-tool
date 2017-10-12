package test.TestJobTrackerHADaemon;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobTracker;
//import org.apache.hadoop.mapred.JobTrackerHADaemon;

/**
 * liulu5
 * 2013-12-10
 */
public class JobTrackerAdapter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//需要hadoop-core-2.3.0-mr1-cdh5.0.0.jar
		/**
		Configuration.addDefaultResource("/home/ocdc/app/mr1/conf/mapred-site.xml");
		JobTrackerHADaemon ha = new JobTrackerHADaemon(new Configuration());
		JobTracker tracker = ha.getJobTracker();
		System.out.println("getAliveNodesInfoJson : " + tracker.getAliveNodesInfoJson());
		try {
			System.out.println("getFilesystemName : " + tracker.getFilesystemName());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("getHostname : " + tracker.getHostname());
		System.out.println("getJobTrackerMachine : " + tracker.getJobTrackerMachine());
		
		List<JobInProgress> jobs = tracker.getCompletedJobs();
		System.out.println("jobs.size() : " + jobs.size());
		for(JobInProgress job : jobs){
			System.out.println(job.getJobID());
		}
		*/
	}

}

