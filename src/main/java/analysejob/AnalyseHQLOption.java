package analysejob;

/**
 * liulu5
 * 2013-11-27
 */
public class AnalyseHQLOption {
	
	String hiveserver = "localhost";
	int port = 10000;
	String db = "default";
	String hql;
	String outputPath;
	
	public String getDb() {
		return db;
	}
	public void setDb(String db) {
		this.db = db;
	}
	public String getHql() {
		return hql;
	}
	public void setHql(String hql) {
		this.hql = hql;
	}
	public String getOutputPath() {
		return outputPath;
	}
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	
	public String getHiveserver() {
		return hiveserver;
	}
	public void setHiveserver(String hiveserver) {
		this.hiveserver = hiveserver;
	}
	
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public void clear(){
		this.setHql(null);
		this.setOutputPath(null);
	}
	
}