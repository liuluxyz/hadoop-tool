package autodeploy.service.config.zookeeper;

import autodeploy.common.host.Host;

/**
 * 
 * @Description: TODO
 * @author liulu5
 * @date 2014-8-8 下午4:28:56
 */
public class ZookeeperConfig {
	
	boolean zookeeperDistributed;//zookeeper是否部署分布式
	String homeDir;
	String dataDir;
	int clientPort;
	Host server;//单机版zookeeper只配置一个server
	Server[] servers;//分布式zookeeper配置多个server
	
	public Server[] getServers() {
		return servers;
	}
	public void setServers(Server[] servers) {
		this.servers = servers;
	}
	public String getHomeDir() {
		return homeDir;
	}
	public void setHomeDir(String homeDir) {
		this.homeDir = homeDir;
	}
	public String getDataDir() {
		return dataDir;
	}
	public void setDataDir(String dataDir) {
		this.dataDir = dataDir;
	}
	public int getClientPort() {
		return clientPort;
	}
	public void setClientPort(int clientPort) {
		this.clientPort = clientPort;
	}
	public Host getServer() {
		return server;
	}
	public void setServer(Host server) {
		this.server = server;
	}
	public boolean isZookeeperDistributed() {
		return zookeeperDistributed;
	}
	public void setZookeeperDistributed(boolean zookeeperDistributed) {
		this.zookeeperDistributed = zookeeperDistributed;
	}

	public static class Server implements Comparable {
		
		private int no;
		private Host host;
		private int connectPort;
		private int electPort;
		
		public Server(){
			
		}
		public Server(int no, Host host, int connectPort, int electPort){
			this.no = no;
			this.host = host;
			this.connectPort = connectPort;
			this.electPort = electPort;
		}
		
		public int getNo() {
			return no;
		}
		public void setNo(int no) {
			this.no = no;
		}
		public Host getHost() {
			return host;
		}
		public void setHost(Host host) {
			this.host = host;
		}
		public int getConnectPort() {
			return connectPort;
		}
		public void setConnectPort(int connectPort) {
			this.connectPort = connectPort;
		}
		public int getElectPort() {
			return electPort;
		}
		public void setElectPort(int electPort) {
			this.electPort = electPort;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(Object o) {
			if(!(o instanceof Server)){
				return -1;
			}
			
			return this.getNo() - ((Server)o).getNo();
		}			
	}
}

