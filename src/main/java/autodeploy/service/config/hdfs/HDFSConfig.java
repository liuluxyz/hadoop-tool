package autodeploy.service.config.hdfs;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import autodeploy.common.host.Host;

/**
 * liulu5
 * 2014-6-17
 */
public class HDFSConfig {

	Host master1;
	Host master2;
	Host[] slaves;
	String homeDir;
	String javaHome;
//	Map<String, String> hadoopEnv;//后期再看是否需要此信息
	Map<String, String> coreSite;
	Map<String, String> hdfsSite;
	
	public String getJavaHome() {
		return javaHome;
	}
	public void setJavaHome(String javaHome) {
		this.javaHome = javaHome;
	}
//	public Map<String, String> getHadoopEnv() {
//		return hadoopEnv;
//	}
//	public void setHadoopEnv(Map<String, String> hadoopEnv) {
//		this.hadoopEnv = hadoopEnv;
//	}
//	public void addHadoopEnv(String key, String value) {
//		if(this.hadoopEnv == null){
//			hadoopEnv = new HashMap<String, String>();
//		}
//		this.hadoopEnv.put(key, value);
//	}
	public Map<String, String> getCoreSite() {
		return coreSite;
	}
	public void setCoreSite(Map<String, String> coreSite) {
		this.coreSite = coreSite;
	}
	public void addCoreSite(String key, String value) {
		if(this.coreSite == null){
			coreSite = new LinkedHashMap<String, String>();
		}
		this.coreSite.put(key, value);
	}
	public Map<String, String> getHdfsSite() {
		return hdfsSite;
	}
	public void setHdfsSite(Map<String, String> hdfsSite) {
		this.hdfsSite = hdfsSite;
	}
	public void addHdfsSite(Map<String, String> hdfsSite) {
		if(this.hdfsSite == null){
			this.hdfsSite = new LinkedHashMap<String, String>();
		}
		this.hdfsSite.putAll(hdfsSite);
	}
	public void addHdfsSite(String key, String value) {
		if(this.hdfsSite == null){
			hdfsSite = new LinkedHashMap<String, String>();
		}
		this.hdfsSite.put(key, value);
	}
	public Host getMaster1() {
		return master1;
	}
	public void setMaster1(Host master) {
		this.master1 = master;
	}
	public Host getMaster2() {
		return master2;
	}
	public void setMaster2(Host master) {
		this.master2 = master;
	}
	public Host[] getSlaves() {
		return slaves;
	}
	public void setSlaves(Host[] slaves) {
		this.slaves = slaves;
	}
	public String getHomeDir() {
		return homeDir;
	}
	public void setHomeDir(String homeDir) {
		this.homeDir = homeDir;
	}
	
}


