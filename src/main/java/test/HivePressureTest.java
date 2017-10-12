package test;

import java.util.List;

import hivetool.HiveConnHelper;

/**
 * liulu5
 * 2014-11-4
 */
/**
 * @Description: TODO
 * @author liulu5
 * @date 2014-11-4 下午6:00:39 
 */
public class HivePressureTest {

	static class MyThread extends Thread{

		String sql;
		public MyThread(String sql){
			this.sql = sql;
		}
		
		@Override
		public void run() {
			HiveConnHelper.getInstance().executeHQL("default", sql);
		}
		
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int num = Integer.parseInt(args[0]);
		List<String> tables = HiveConnHelper.getInstance().getAllTables("default");
		while(--num > 0){
			for(String table : tables){
				String sql = "select count(*) from " + table;
				MyThread myThread = new MyThread(sql);
				myThread.start();
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

}

