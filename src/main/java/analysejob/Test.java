package analysejob;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

import analysejob.adapter.MRAdapter;
import analysejob.common.AnalyseResult;
import analysejob.rule.job.JobAnalyseRule;
import analysejob.rule.job.RuleJobDataSkew;
import analysejob.rule.job.RuleMapHeap;
import analysejob.rule.job.RuleMapReadBytesTooBig;
import analysejob.rule.job.RuleMapReadBytesTooSmall;
import analysejob.rule.job.RuleMapRunTooQuick;
import analysejob.rule.job.RuleMapRunTooSlow;
import analysejob.rule.job.RuleReduceHeap;
import analysejob.rule.job.RuleReduceReadBytesTooBig;
import analysejob.rule.job.RuleReduceRunTooSlow;

/**
 * liulu5
 * 2013-12-12
 */
public class Test {

	public static void printDesc() throws Exception{
		MRAdapter mrAdapter = new MRAdapter();
		JobAnalyseRule[] analyses = new JobAnalyseRule[]{
				new RuleMapReadBytesTooBig(mrAdapter),
				new RuleMapReadBytesTooSmall(mrAdapter),
				new RuleMapRunTooQuick(mrAdapter),
				new RuleMapRunTooSlow(mrAdapter), 
				new RuleReduceReadBytesTooBig(mrAdapter),
				new RuleReduceRunTooSlow(mrAdapter), 
				new RuleMapHeap(mrAdapter),
				new RuleReduceHeap(mrAdapter),
				new RuleJobDataSkew(mrAdapter)
		};
		for(int i=0; i<analyses.length; i++){
			System.out.println("规则 : " + analyses[i].toString());
//			System.out.println("说明 : " + analyses[i].getDesc());
//			System.out.println("原因 : ");
//			for(int j=0; j<analyses[i].getReason().length; j++){
//				System.out.println("	" + (j+1) + ". " + analyses[i].getReason()[j]);
//			}
//			System.out.println("建议 : ");
//			for(int j=0; j<analyses[i].getSuggestion().length; j++){
//				System.out.println("	" + (j+1) + ". " + analyses[i].getSuggestion()[j]);
//			}
//			System.out.println("---------------------------------------------------");
		}
	}
	
	public static void printUsage(){
		new AnalyseJob().printUsage();
	}
	
	private static void parseHQL(){
		String hql = "select t1.a, t2.b, t3.c, t4.d, t5.e " +
				"FROM (select * from trans_ods_cust_newmsg_ds_temp_1) t1 " +
				"LEFT OUTER JOIN Map_CityCounty t2 ON (t1.OWNERAREAID = t2.OLD_COUNTY) " +
				"LEFT OUTER JOIN ODS_ORGANIZATION_MSG_DS_SUB t3 ON (t1.OWNERAREAID = t3.channel_id) " +
				"LEFT OUTER JOIN usys_etl_map_total t4 ON (t4.map_name = 'FMAP_CUST_IDEN' AND t1.CERTTYPE = t4.boss_value) " +
				"LEFT OUTER JOIN usys_etl_map_total t5 ON (t5.map_name = 'FMAP_PUB_STATUS' AND upper(trim(t1. STATUS)) = upper(t5.boss_value))";
		Reader statementReader = new StringReader(hql);
		
		CCJSqlParserManager parser = new CCJSqlParserManager();
		try {
			Statement state = parser.parse(statementReader);
//			state.accept(statementVisitor);
			String a = state.toString();
			System.out.println(a);
			
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void testJobhistory() {
		Configuration conf = new Configuration();
//		conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\analyse\\mapred-site.xml"));
		JobHistory his = new JobHistory();
		his.init(conf);
		
		Map<JobId, Job> jobs = his.getAllJobs();
		System.out.println("job num : " + jobs.size());
		
	}
	
	public static void testYarnClient() throws YarnException, IOException{
		Configuration conf = new Configuration();
		conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\analyse\\mapred-site.xml"));
		conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\analyse\\yarn-site.xml"));
		YarnClient client = YarnClient.createYarnClient();
		client.init(conf);
		try{
			client.start();	
		}catch (Exception e){
			System.out.println(e.toString());
		}
		
		List<QueueInfo> queue = client.getAllQueues();
		System.out.println(queue.size());
		
		List<ApplicationReport> apps = client.getApplications();
		
		System.out.println(apps.size());
		System.out.println(apps.get(0).getApplicationId());
		
		System.out.println(apps.get(0).getApplicationType());
		System.out.println(apps.get(0).getDiagnostics());
		System.out.println(apps.get(0).getHost());
		System.out.println(apps.get(0).getName());
		System.out.println(apps.get(0).getOriginalTrackingUrl());
		System.out.println(apps.get(0).getProgress());
		System.out.println(apps.get(0).getQueue());
		System.out.println(apps.get(0).getRpcPort());
		
		
		org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> token = client.getAMRMToken(apps.get(0).getApplicationId());
		
		
	}
	
	public static void testJobClient(){
		try {
			Configuration conf = new Configuration();
			conf.addResource("C:\\Users\\liulu5\\Desktop\\analyse\\mapred-site.xml");
			conf.addResource("C:\\Users\\liulu5\\Desktop\\analyse\\yarn-site.xml");
			JobClient jobClient = new JobClient(conf);
			
//			JobClient jobClient = new JobClient(new JobConf(conf));	
			
			int num = jobClient.getAllJobs().length;
			System.out.println(num);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			long size = fs.getDefaultBlockSize();
			System.out.println(size);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			testJobhistory();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

