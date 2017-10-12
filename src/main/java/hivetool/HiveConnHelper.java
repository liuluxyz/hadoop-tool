package hivetool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.ResultData;

/**
 * Hive Connection Helper
 * description:
 * @author 
 */
public class HiveConnHelper {
	
	private static final Log log = LogFactory.getLog(HiveConnHelper.class);
	
	private static final String hiveserverDefaultIP = "localhost";
//	private static final String driver = "org.apache.hadoop.hive.jdbc.HiveDriver";
//	private static final String url = "jdbc:hive://192.168.11.70:10000/default";
	
	private static final String driver = "org.apache.hive.jdbc.HiveDriver";
	private static final String url = "jdbc:hive2://localhost:10000/default";
	
	private static final String user = "";
	private static final String password = "";
	
	private static HiveConnHelper hiveConnHelper;
	
	private HiveConnHelper(){
		init();
	}
	
	public static HiveConnHelper getInstance(){
		if(hiveConnHelper == null){
			hiveConnHelper = new HiveConnHelper();
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
		}
//		catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			LOG.error("[HiveConnHelper.java -- init]:" + e.toString());
//		}
	}
	
	public Connection getConnection(){
		log.info("start getConnection...");
		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("[HiveConnHelper.java -- getConnection]", e);
		}
		return null;
	}
	
	public Connection getConnection(String hiveserverIp, int port){
		log.info("start getConnection...");
		log.info("hive server ip is : " + hiveserverIp);
		try {
			String url = "jdbc:hive2://" + hiveserverIp + ":" + port + "/default";
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
//			LOG.error("[HiveConnHelper.java -- getConnection]", e);
		}
		return null;
	}
	
	public Connection getConnection(String hiveserverIp){
		log.info("start getConnection...");
		log.info("hive server ip is : " + hiveserverIp);
		try {
			String url = "jdbc:hive2://" + hiveserverIp + ":10000/default";
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("[HiveConnHelper.java -- getConnection]", e);
		}
		return null;
	}
	
	
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
		}
	}
	
	public List<String> getAllTables(String db){
		return executeQuery(db, "show tables");
	}
	
	public List<String> executeSingleColumnQuery(String db, String sql){
		List<String> temp = new ArrayList<String>();
		Connection conn = HiveConnHelper.getInstance().getConnection();
		Statement state = null;
		ResultSet set = null;
		log.info("executeQuery : " + sql);
		try {
			state = conn.createStatement();
			state.execute("use " + db);
			set = state.executeQuery(sql);
			while(set.next()){
				temp.add(set.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			HiveConnHelper.getInstance().close(state, set);
			HiveConnHelper.getInstance().close(conn);
		}
		return temp;
	}
	
	public int queryTableCountValue(String db, String tablename){
		Connection conn = HiveConnHelper.getInstance().getConnection();
		Statement state = null;
		ResultSet set = null;
		try {
			state = conn.createStatement();
			set = state.executeQuery("select count(1) from " + db + "." + tablename);
			while(set.next()){
				return set.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			HiveConnHelper.getInstance().close(state, set);
			HiveConnHelper.getInstance().close(conn);
		}
		return 0;
	}
	
	public List<String> executeQuery(String db, String sql){
		List<String> list = new ArrayList<String>();
		Connection conn = HiveConnHelper.getInstance().getConnection();
		log.info("conn : " + conn);
		Statement state = null;
		ResultSet set = null;
		log.info("executeQuery : " + sql);
		try {
			state = conn.createStatement();
			state.execute("use " + db);
			set = state.executeQuery(sql);
			
			while(set.next()){
				String str = set.getString(1);
				list.add(str);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error(e.toString(), e);
			return list;
		}finally{
			HiveConnHelper.getInstance().close(state, set);
			HiveConnHelper.getInstance().close(conn);
		}
		return list;
	}
	
	/**
	 * query
	 * @param db
	 * @param sql
	 * @param fieldNum:  字段个数
	 * @return
	 * @throws Exception 
	 */
	public List<String[]> executeQuery(String sql, int fieldNum) throws Exception{
		List<String[]> list = new ArrayList<String[]>();
		Connection conn = this.getConnection();
		Statement state = null;
		ResultSet set = null;
		log.info("executeQuery : " + sql);
		try {
			state = conn.createStatement();
			set = state.executeQuery(sql);
			if(fieldNum != set.getMetaData().getColumnCount()){
				throw new Exception("查询的字段个数与指定的字段个数不同");
			}
			while(set.next()){
				String[] values = new String[fieldNum];
				for(int i=0; i<fieldNum; i++){
					values[i] = set.getString(i+1);
				}
				list.add(values);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error(e.toString(), e);
			return list;
		}finally{
			HiveConnHelper.getInstance().close(state, set);
			HiveConnHelper.getInstance().close(conn);
		}
		return list;
	}
	
	public List<String[]> executeMultiQuery(String db, String sql) throws Exception{
		log.info("start executeMultiQuery...");
		List<String[]> list = new ArrayList<String[]>();
		Connection conn = this.getConnection();
		Statement state = null;
		ResultSet set = null;
		log.info("executeQuery : " + sql);
		try {
			state = conn.createStatement();
			state.execute("use " + db);
			set = state.executeQuery(sql);
			int colCount = set.getMetaData().getColumnCount();
			while(set.next()){
				String[] values = new String[colCount];
				for(int i=0; i<colCount; i++){
					values[i] = set.getString(i+1);
				}
				list.add(values);
			}
		} catch (SQLException e) {
			log.error(e.toString(), e);
			return list;
		}finally{
			HiveConnHelper.getInstance().close(state, set);
			HiveConnHelper.getInstance().close(conn);
		}
		log.info("end executeMultiQuery...");
		return list;
	}
	
	public List<String[]> executeQuery(String hiveserverIp, String db, String sql, int fieldNum) throws Exception{
		return executeQuery(hiveserverIp, 10000, db, sql, fieldNum);
	}
	
	public List<String[]> executeQuery(String hiveserverIp, int port, String db, String sql, int fieldNum) throws Exception{
		List<String[]> list = new ArrayList<String[]>();
		Connection conn = this.getConnection(hiveserverIp, port);
		Statement state = null;
		ResultSet set = null;
		log.info("executeQuery : " + sql);
		try {
			state = conn.createStatement();
			state.execute("use " + db);
			set = state.executeQuery(sql);
			if(fieldNum != set.getMetaData().getColumnCount()){
				throw new Exception("查询的字段个数与指定的字段个数不同");
			}
			while(set.next()){
				String[] values = new String[fieldNum];
				for(int i=0; i<fieldNum; i++){
					values[i] = set.getString(i+1);
				}
				list.add(values);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error(e.toString(), e);
			return list;
		}finally{
			HiveConnHelper.getInstance().close(state, set);
			HiveConnHelper.getInstance().close(conn);
		}
		return list;
	}
	
	/**
	 * 
	 * @param db
	 * @param sql
	 * @param fieldNum
	 * @return
	 * @throws Exception
	 */
	public List<String[]> executeQuery(String db, String sql, int fieldNum) throws Exception{
		return executeQuery(hiveserverDefaultIP, db, sql, fieldNum);
	}
	
	public List<String> executeQuery(String db, String sql, String labelName){
		List<String> list = new ArrayList<String>();
		Connection conn = HiveConnHelper.getInstance().getConnection();
		Statement state = null;
		ResultSet set = null;
		log.info("executeQuery : " + sql);
		try {
			state = conn.createStatement();
			state.execute("use " + db);
			set = state.executeQuery(sql);
			while(set.next()){
				String str = set.getString(labelName).trim();
				list.add(str);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error(e.toString(), e);
			return list;
		}finally{
			HiveConnHelper.getInstance().close(state, set);
			HiveConnHelper.getInstance().close(conn);
		}
		return list;
	}
	
	private static void executeHQLByThread(final String hql){
		new Thread(){
			public void run(){
				long a = System.currentTimeMillis();
				Connection conn = HiveConnHelper.getInstance().getConnection();
				long b = System.currentTimeMillis();
				
				Statement state = null;	
				try {
					state = conn.createStatement();
					long c = System.currentTimeMillis();
//					log.info("begin to execute..." + new java.util.Date(c).toLocaleString());
					log.info("begin execute : " + hql);
					state.execute(hql);
					log.info("end execute : " + hql);
					long d = System.currentTimeMillis();			
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}finally{
					HiveConnHelper.getInstance().close(state);
					HiveConnHelper.getInstance().close(conn);
				}
			}
		}.start();
	}
	
	public ResultData executeHQL(final String... hqls){
		Connection conn = HiveConnHelper.getInstance().getConnection();
		Statement state = null;	
		try {
			state = conn.createStatement();
			for(String hql : hqls){
				boolean res = state.execute(hql);
				if(res == false){
					return new ResultData(res);
				}
			}
			return new ResultData(true);
		} catch (SQLException e) {
//			e.printStackTrace();
			log.error("executeHQL error : " + Arrays.toString(hqls), e);
			return new ResultData(false, "executeHQL error : [" + Arrays.toString(hqls) + "] [" + e.toString() + "]");
		}finally{
			HiveConnHelper.getInstance().close(state);
			HiveConnHelper.getInstance().close(conn);
		}
	}
	
	public ResultData executeHQL(String db, final String hql){
		Connection conn = HiveConnHelper.getInstance().getConnection();
		Statement state = null;	
		try {
			state = conn.createStatement();
			state.execute("use " + db);
			boolean res = state.execute(hql);
			return new ResultData(res);
		} catch (SQLException e) {
//			e.printStackTrace();
			log.error("executeHQL error : " + hql, e);
			return new ResultData(false, "executeHQL error : [" + hql + "] [" + e.toString() + "]");
		}finally{
			HiveConnHelper.getInstance().close(state);
			HiveConnHelper.getInstance().close(conn);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
