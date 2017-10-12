package autodeploy.service;

import autodeploy.common.host.Host;

/**
 * liulu5
 * 2014-6-16
 */
public interface Service {
	
//	protected Host masterHost;
//	protected Host[] slaves;
//	protected String homeDir;
	
	public abstract void start() throws Exception;
	
	public abstract void stop() throws Exception;

	public abstract String getName();

//	public String getHomeDir() {
//		return homeDir;
//	}
//	public void setHomeDir(String homeDir) {
//		this.homeDir = homeDir;
//	}
//
//	public Host getMasterHost() {
//		return masterHost;
//	}
//
//	public void setMasterHost(Host masterHost) {
//		this.masterHost = masterHost;
//	}
//
//	public Host[] getSlaves() {
//		return slaves;
//	}
//
//	public void setSlaves(Host[] slaves) {
//		this.slaves = slaves;
//	}
	
}
