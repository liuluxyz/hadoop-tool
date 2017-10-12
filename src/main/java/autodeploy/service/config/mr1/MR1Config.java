package autodeploy.service.config.mr1;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import autodeploy.common.host.Host;

/**
 * 
 * @author liulu5
 *
 */
public class MR1Config {

	Host master;
	Host[] slaves;
	String homeDir;
	String javaHome;
//	Map<String, String> hadoopEnv;
	Map<String, String> mapredSite;
//	String[] slaves;
	
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
	public Map<String, String> getMapredSite() {
		return mapredSite;
	}
	public void setMapredSite(Map<String, String> mapredSite) {
		this.mapredSite = mapredSite;
	}
	public void addMapredSite(String key, String value) {
		if(this.mapredSite == null){
			mapredSite = new LinkedHashMap<String, String>();
		}
		this.mapredSite.put(key, value);
	}
//	public String[] getSlaves() {
//		return slaves;
//	}
//	public void setSlaves(String[] slaves) {
//		this.slaves = slaves;
//	}
	public Host getMaster() {
		return master;
	}
	public void setMaster(Host master) {
		this.master = master;
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

