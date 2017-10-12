package autodeploy.service;

import autodeploy.common.host.Host;
import autodeploy.service.config.zookeeper.ZookeeperConfig;
import net.neoremind.sshxcute.core.Result;

import common.linux.ExecuteLinuxSSHCMD;

/**
 * 
 * @Description: TODO
 * @author liulu5
 * @date 2014-8-6 下午2:28:58
 */
public class ZookeeperService implements Service {
	
	ZookeeperConfig zookeeperConfig;
	
	public ZookeeperService(ZookeeperConfig zookeeperConfig){
		this.zookeeperConfig = zookeeperConfig;
	}
	
	@Override
	public void start() throws Exception {
		Host[] hosts = parseServiceHosts();
		for(Host host : hosts){
			ExecuteLinuxSSHCMD execute = null;
			try{
				execute = new ExecuteLinuxSSHCMD(host);
				String[] cmds = new String[]{
						"source /home/" + host.getUsername() + "/.bash_profile",
						"cd " + zookeeperConfig.getHomeDir() + "/bin",
						"./zkServer.sh start"
				};
				Result res = execute.execute(cmds);
				if(!res.isSuccess){
					throw new Exception("execute start zookeeper service cmd failed : " + res.error_msg);
				}
			}finally{
				if(execute != null)
					execute.close();
			}
		}
	}

	@Override
	public void stop() throws Exception {
		Host[] hosts = parseServiceHosts();
		for(Host host : hosts){
			ExecuteLinuxSSHCMD execute = null;
			try{
				execute = new ExecuteLinuxSSHCMD(host);
				String[] cmds = new String[]{
						"cd " + zookeeperConfig.getHomeDir() + "/bin",
						"./zkServer.sh stop"
				};
				Result res = execute.execute(cmds);
				if(!res.isSuccess){
					throw new Exception("execute start zookeeper service cmd failed : " + res.error_msg);
				}
			}finally{
				if(execute != null)
					execute.close();
			}
		}
	}

	private Host[] parseServiceHosts(){
		if(zookeeperConfig.isZookeeperDistributed()){
			Host[] hosts = new Host[zookeeperConfig.getServers().length];
			for(int i=0; i<zookeeperConfig.getServers().length; i++){
				hosts[i] = zookeeperConfig.getServers()[i].getHost();
			}
			return hosts;
		}else{
			return new Host[]{zookeeperConfig.getServer()};
		}
	}
	
	@Override
	public String getName() {
		return "zookeeper";
	}
}

