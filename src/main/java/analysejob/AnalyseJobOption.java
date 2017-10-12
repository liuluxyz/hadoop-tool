package analysejob;

/**
 * liulu5
 * 2013-11-27
 */
public class AnalyseJobOption {
	
	String jobid;
	String jobname;
	Match jobnameMatch;
	String hiveql;
	Match hiveqlMatch;
	String mrConf;
	String outputPath;
	int mapmin = 100;
	int jobtime = 2;
	
	public static enum Match{ equal, contain };//匹配方式：完全匹配 or 部分匹配,

	public String getJobid() {
		return jobid;
	}
	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	public String getJobname() {
		return jobname;
	}
	public void setJobname(String jobname) {
		this.jobname = jobname;
	}
	public Match getJobnameMatch() {
		return jobnameMatch;
	}
	public void setJobnameMatch(Match jobnameMatch) {
		this.jobnameMatch = jobnameMatch;
	}
	public String getHiveql() {
		return hiveql;
	}
	public void setHiveql(String hiveql) {
		this.hiveql = hiveql;
	}
	public Match getHiveqlMatch() {
		return hiveqlMatch;
	}
	public void setHiveqlMatch(Match hiveqlMatch) {
		this.hiveqlMatch = hiveqlMatch;
	}
	public String getMrConf() {
		return mrConf;
	}
	public void setMrConf(String mrConf) {
		this.mrConf = mrConf;
	}
	public String getOutputPath() {
		return outputPath;
	}
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	public int getMapmin() {
		return mapmin;
	}
	public void setMapmin(int mapmin) {
		this.mapmin = mapmin;
	}
	public int getJobtime() {
		return jobtime;
	}
	public void setJobtime(int jobtime) {
		this.jobtime = jobtime;
	}
	public void clear(){
		this.setJobid(null);
		this.setJobname(null);
		this.setJobnameMatch(null);
		this.setHiveql(null);
		this.setHiveqlMatch(null);
		this.setMrConf(null);
		this.setOutputPath(null);
		this.setMapmin(100);
		this.setJobtime(2);
	}
	
}