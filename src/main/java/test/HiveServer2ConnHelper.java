package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Hive Connection Helper
 * description:
 * @author liulu5 2012-8-11 下午2:32:31
 */
public class HiveServer2ConnHelper {
	private static final Log LOG = LogFactory.getLog(HiveServer2ConnHelper.class);
	
	private static final String driver = "org.apache.hive.jdbc.HiveDriver";
//	private static final String driver = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	private static final String hiveserverDefaultIP = "localhost";
	
	private static final String url = "jdbc:hive2://132.63.10.82:10000/default";
	
	private static final String user = "";
	private static final String password = "";
	
//	private Connection conn;
	
	private static HiveServer2ConnHelper hiveConnHelper;
	
	private HiveServer2ConnHelper(){
		init();
	}
	
	public static HiveServer2ConnHelper getInstance(){
		if(hiveConnHelper == null){
			hiveConnHelper = new HiveServer2ConnHelper();
		}
		return hiveConnHelper;
	}
	
	public void init(){
		try {
			Class.forName(driver);
//			conn = DriverManager.getConnection(url, user, password);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("[HiveConnHelper.java -- init]:" + e.toString());
		}
//		catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			LOG.error("[HiveConnHelper.java -- init]:" + e.toString());
//		}
	}
	
	public Connection getConnection(){
		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
//			LOG.error("[HiveConnHelper.java -- getConnection]", e);
		}
		return null;
	}
	
//	public Statement getStatement(){
//		if(conn == null){
//			init();
//		}
//		try {
//			return conn.createStatement();
//		} catch (SQLException e) {
//			e.printStackTrace();
//			LOG.error("[HiveConnHelper.java -- getStatement]:" + e.toString());
//		}
//		return null;
//	}
	
	public void close(Statement state, ResultSet result){
		try {
			if(result != null){
				result.close();
			}
			if(state != null){
				state.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			result = null;
			state = null;
			LOG.error("HiveConnHelper.close", e);
		}
	}
	
	public void close(Connection conn){
		try {
			if(conn != null){
				conn.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			conn = null;
			LOG.error("HiveConnHelper.close", e);
		}
	}
	
	public void close(Statement state){
		try {
			if(state != null){
				state.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			state = null;
			LOG.error("HiveConnHelper.close", e);
		}
	}
	
	private static void showTables(){
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
		Statement state = null;
		ResultSet set = null;
		try {
			state = conn.createStatement();
			set = conn.getMetaData().getTables(null, null, null, new String[]{"TABLES"});
			 System.out.println("aaaaaaa");  
			while(set.next()){
				String tt = set.getString(1);
		        System.out.println(tt);  
			}
			 System.out.println("aaaaaaaaaa");  
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			HiveServer2ConnHelper.getInstance().close(state, set);
			HiveServer2ConnHelper.getInstance().close(conn);
		}
	}
	
	private static void executeQuery(String sql){
//		long a = System.currentTimeMillis();
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
//		long b = System.currentTimeMillis();
		Statement state = null;
		ResultSet set = null;
		try {
			state = conn.createStatement();
//			long c = System.currentTimeMillis();
//			System.out.println("begin to execute..." + new java.util.Date(c).toLocaleString());
			set = state.executeQuery(sql);
//			long d = System.currentTimeMillis();
//			System.out.println("end to execute..." + new java.util.Date(d).toLocaleString());
//			System.out.println("execute hive sql : " + (d-c));
//			ResultSet set = conn.getMetaData().getTables(null, null, null, new String[]{"TABLE"});
			while(set.next()){
				String tt = set.getString(1);
		        System.out.println(tt);  
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			HiveServer2ConnHelper.getInstance().close(state, set);
			HiveServer2ConnHelper.getInstance().close(conn);
		}
	}

	private static void execute(String sql){
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
		Statement state = null;
		ResultSet set = null;
		try {
			state = conn.createStatement();
			boolean a = state.execute(sql);
			System.out.println("res : " + a);
//			while(set.next()){
//				String tt = set.getString(1);
//		        System.out.println(tt);  
//			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			HiveServer2ConnHelper.getInstance().close(state, set);
			HiveServer2ConnHelper.getInstance().close(conn);
		}
	}

	
	private static void executeQueryAndPrint(String sql){
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
		Statement state = null;
		ResultSet set = null;
		try {
			state = conn.createStatement();
			set = state.executeQuery(sql);
			int colCount = set.getMetaData().getColumnCount();
			while(set.next()){
				int col = 0;
				while(col++ < colCount){
					String tt = set.getString(col);
					System.out.print(tt);
					System.out.print("	");
				}
				System.out.println();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			HiveServer2ConnHelper.getInstance().close(state, set);
			HiveServer2ConnHelper.getInstance().close(conn);
		}
	}
	
	private static void executeQueryAndPrintColNameAndValue(String sql){
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
		Statement state = null;
		ResultSet set = null;
		try {
			state = conn.createStatement();
			set = state.executeQuery(sql);
			int colCount = set.getMetaData().getColumnCount();
			
			while(set.next()){
				int col = 0;
				while(col++ < colCount){
					String colLabel = set.getMetaData().getColumnLabel(col);
					String tt = set.getString(col);
					System.out.println(colLabel + " : " + tt);
				}
				System.out.println("-----------");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			HiveServer2ConnHelper.getInstance().close(state, set);
			HiveServer2ConnHelper.getInstance().close(conn);
		}
	}
	
	private static boolean setHQLConfig(Statement state){
		String[] setCMDs = new String[]{
				"set mapred.max.split.size=268435456",
				"set mapred.min.split.size.per.node=268435456",
				"set mapred.min.split.size.per.rack=268435456",
				"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat",
//				"add jar /home/ocdc/bin/hive-0.9.0-bin/lib/hive-rownumber.jar", 
//				"create temporary function row_number as 'com.hive.row_number'", 
//				"set mapred.job.ocdc.priority=120"
		};
		boolean res = true;
		try {
			for(String cmd : setCMDs){
				state.execute(cmd);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return res;
	}

	private static void testConnection(){
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
		Statement state = null;
		try {
			state = conn.createStatement();
//			ResultSet set = state.executeQuery("select count(1) from yangjp");
			ResultSet set = state.executeQuery("select empty(a, 'xxx') from dual");
			while(set.next()){
				String str = set.getString(1);
				System.out.println("get count : " + str);
				System.out.println("conn is ok");
				break;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void test(){
		String sql = "select count(1) from a";
		
		long a = System.currentTimeMillis();
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
		long b = System.currentTimeMillis();
		System.out.println("get hive connection: " + (b-a));
		Statement state = null;
		try {
			state = conn.createStatement();
			long c = System.currentTimeMillis();
			System.out.println("begin to execute..." + new java.util.Date(c).toLocaleString());
			ResultSet set = state.executeQuery(sql);
			long d = System.currentTimeMillis();
			System.out.println("end to execute..." + new java.util.Date(d).toLocaleString());
			System.out.println("execute hive sql : " + (d-c));
//			ResultSet set = conn.getMetaData().getTables(null, null, null, new String[]{"TABLE"});
			while(set.next()){
				String tt = set.getString(1);  
		        System.out.println(tt);  
			}
//			conn.getCatalog();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void executeHQLByThread(final String hql){
		new Thread(){
			public void run(){
				long a = System.currentTimeMillis();
				Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
				long b = System.currentTimeMillis();
				
				Statement state = null;	
				try {
					state = conn.createStatement();
					long c = System.currentTimeMillis();
//					System.out.println("begin to execute..." + new java.util.Date(c).toLocaleString());
					System.out.println("begin execute : " + hql);
					state.execute(hql);
					System.out.println("end execute : " + hql);
					long d = System.currentTimeMillis();			
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally{
					HiveServer2ConnHelper.getInstance().close(state);
					HiveServer2ConnHelper.getInstance().close(conn);
				}
			}
		}.start();
	}
	
	public static void testThread(){
		new Thread(){
			public void run(){
				int num = 0;
				while(true){
					System.out.println("test while : " + (num++));
					testConnection();
					System.out.println("-----------------------------------------");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
	}
	
	private static void executeHQL(final String hql){

		long a = System.currentTimeMillis();
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
		long b = System.currentTimeMillis();
		
		Statement state = null;	
		try {
			state = conn.createStatement();
			setHQLConfig(state);
			
			long c = System.currentTimeMillis();
//			System.out.println("begin to execute..." + new java.util.Date(c).toLocaleString());
			System.out.println("begin execute : " + hql);
			state.execute(hql);
			System.out.println("end execute : " + hql);
			long d = System.currentTimeMillis();			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			HiveServer2ConnHelper.getInstance().close(state);
			HiveServer2ConnHelper.getInstance().close(conn);
		}
	}
	
	private static void executeUpdateHQL(String hql){
		Connection conn = HiveServer2ConnHelper.getInstance().getConnection();
		Statement state = null;	
		try {
			state = conn.createStatement();
			state.execute(hql);
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			HiveServer2ConnHelper.getInstance().close(state);
			HiveServer2ConnHelper.getInstance().close(conn);
		}
	}
	
	private static void queryCount(){
		String[] hqls = new String[]{
				"select count(1) from hivetest1",
				"select count(1) from hivetest2",
				"select count(1) from hivetest3",
				"select count(1) from hivetest4",
				"select count(1) from hivetest5",
				"select count(1) from hivetest6",
				"select count(1) from hivetest7",
				"select count(1) from hivetest_tmp1",
				"select count(1) from hivetest_tmp2",
				"select count(1) from hivetest_tmp3",
				"select count(1) from hivetest_tmp4",
				"select count(1) from hivetest_tmp5",
				"select count(1) from hivetest_tmp6",
				"select count(1) from hivetest_tmp7",
				"select count(1) from hivetest_tmp8",
		};
		for(String hql : hqls){
			String tablename = hql.substring(hql.indexOf("hivetest"), hql.length()).trim();
			System.out.print(tablename + " count is:");
			executeQuery(hql);	
		}
	}
	
	private static void clearTable(){
		String[] hqls = new String[]{
				"INSERT OVERWRITE TABLE hivetest1 SELECT a.* FROM hivetest_tmp8 a",
				"INSERT OVERWRITE TABLE hivetest2 SELECT a.* FROM hivetest_tmp8 a",
				"INSERT OVERWRITE TABLE hivetest3 SELECT a.* FROM hivetest_tmp8 a",
				"INSERT OVERWRITE TABLE hivetest4 SELECT a.* FROM hivetest_tmp8 a",
				"INSERT OVERWRITE TABLE hivetest5 SELECT a.* FROM hivetest_tmp8 a",
				"INSERT OVERWRITE TABLE hivetest6 SELECT a.* FROM hivetest_tmp8 a",
				"INSERT OVERWRITE TABLE hivetest7 SELECT a.* FROM hivetest_tmp8 a",
		};
		for(String hql : hqls){
			String tablename = hql.substring(hql.indexOf("hivetest"), hql.indexOf("SELECT")).trim();
			System.out.println("clearing table : " + tablename);
			executeUpdateHQL(hql);
			
			System.out.print(tablename + " count is:");
			String selectHQL = "SELECT count(1) FROM " + tablename;
			executeQuery(selectHQL);	
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
//		testThread();
		
//		testConnection();
		
		execute("insert overwrite local directory '/home/ocetl/liulu' select * from ht6");
		
//		executeQueryAndPrint("select *, row_num() from a");
		
//		showTables();
		
		
		String sql = "select " +
				"max(case sequence_id when 1 then average_value END) incomeaverage_value, " +
				"max(case sequence_id when 1 then normal_value END)incomenormal_value, " +
				"max(case sequence_id when 1 then right_value END) incomeright_value, " +
				"max(case sequence_id when 2 then average_value END) infoaverage_value, " +
				"max(case sequence_id when 2 then normal_value END)infonormal_value, " +
				"max(case sequence_id when 2 then right_value END) inforight_value," +
				"max(case sequence_id when 3 then average_value END) unionaverage_value, " +
				"max(case sequence_id when 3 then normal_value END) unionnormal_value, " +
				"max(case sequence_id when 3 then right_value END) unionright_value," +
				"max(case sequence_id when 4 then average_value END) employeeaverage_value, " +
				"max(case sequence_id when 4 then normal_value END) employeenormal_value, " +
				"max(case sequence_id when 4 then right_value END) employeeright_value," +
				"max(case sequence_id when 5 then average_value END) vpmnaverage_value, " +
				"max(case sequence_id when 5 then normal_value END) vpmnnormal_value, " +
				"max(case sequence_id when 5 then right_value END) vpmnright_value," +
				"max(case sequence_id when 6 then average_value END) productaverage_value, " +
				"max(case sequence_id when 6 then normal_value END) productnormal_value, " +
				"max(case sequence_id when 6 then right_value END) productright_value," +
				"max(case sequence_id when 7 then average_value END) infolevelaverage_value, " +
				"max(case sequence_id when 7 then normal_value END) infolevelnormal_value, " +
				"max(case sequence_id when 7 then right_value END) infolevelright_value," +
				"max(case sequence_id when 8 then average_value END) signtimeaverage_value, " +
				"max(case sequence_id when 8 then normal_value END) signtimenormal_value, " +
				"max(case sequence_id when 8 then right_value END) signtimeright_value," +
				"max(case sequence_id when 9 then average_value END) offarpuaverage_value, " +
				"max(case sequence_id when 9 then normal_value END) offarpunormal_value, " +
				"max(case sequence_id when 9 then right_value END) offarpuright_value," +
				"max(case sequence_id when 10 then average_value END) vipaverage_value, " +
				"max(case sequence_id when 10 then normal_value END) vipnormal_value, " +
				"max(case sequence_id when 10 then right_value END) vipright_value," +
				"max(case sequence_id when 11 then average_value END) useraverage_value, " +
				"max(case sequence_id when 11 then normal_value END) usernormal_value, " +
				"max(case sequence_id when 11 then right_value END) userright_value" +
				" from (SELECT 1 as num,sequence_id, average_value, normal_value, right_value FROM ODS_ENTERPRISE_RIGTHPARA_MS) temp " +
				"group by num";
				
				
//		String sql = "select * from liulu";
//		executeQueryAndPrintColNameAndValue(sql);
		
//		clearTable();
		
//		queryCount();
		
//		testIOException();
		
//		String hql1 = "select count(1) from a";
//		executeQuery(hql1);
		
//		test();
		
		/**
		String hql1 = "insert overwrite table a1 select * from a";
		String hql2 = "insert overwrite table b1 select * from b";
		String hql3 = "insert overwrite table c1 select * from c";
		String hql4 = "insert overwrite table d1 select * from d";
		String hql5 = "insert overwrite table e1 select * from e";
		
		executeHQLByThread(hql1);
		executeHQLByThread(hql2);
		executeHQLByThread(hql3);
		executeHQLByThread(hql4);
		executeHQLByThread(hql5);
		*/
		
		/**
		int num = 0;
		for(int i=0; i<500; i++){
			String hql11 = "insert into table a select * from b";
			String hql22 = "insert into table b select * from a";
			executeHQL(hql11);
			executeHQL(hql22);
			System.out.println(num++);
		}
		*/
		
		/**
		String hql11 = "insert into table c select * from a";
		String hql22 = "insert into table d select * from a";
		String hql33 = "insert into table e select * from a";
		executeHQL(hql11);
		executeHQL(hql22);
		executeHQL(hql33);
		*/
		
		
	}

}
