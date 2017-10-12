package autodeploy.service.config.yarn;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import autodeploy.common.host.Host;

/**
 * 
 * @author liulu5
 *
 */
public class YARNConfig {

	Host master;
	Host[] slaves;
	String homeDir;
	Map<String, String> mapredSite;
	Map<String, String> yarnSite;
//	String[] slaves;
	
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
	public Map<String, String> getYarnSite() {
		return yarnSite;
	}
	public void setYarnSite(Map<String, String> yarnSite) {
		this.yarnSite = yarnSite;
	}
	public void addYarnSite(String key, String value) {
		if(this.yarnSite == null){
			yarnSite = new LinkedHashMap<String, String>();
		}
		this.yarnSite.put(key, value);
	}
	public void addYarnSite(Map<String, String> yarnSite) {
		if(yarnSite == null || yarnSite.size() == 0){
			return;
		}
		if(this.yarnSite == null){
			this.yarnSite = yarnSite;
		}else{
			this.yarnSite.putAll(yarnSite);
		}
	}
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

