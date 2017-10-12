package autodeploy.service;

import net.neoremind.sshxcute.core.Result;

import common.linux.ExecuteLinuxSSHCMD;

import autodeploy.service.Service;
import autodeploy.service.config.hdfs.HDFSConfig;

/**
 * liulu5
 * 2014-6-16
 */
public class HDFSService implements Service {
	
	HDFSConfig hdfsConfig;
	public HDFSService(HDFSConfig hdfsConfig){
		this.hdfsConfig = hdfsConfig;
	}
	
	public void start() throws Exception {
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(this.hdfsConfig.getMaster1().getIp(), this.hdfsConfig.getMaster1().getSshPort(), this.hdfsConfig.getMaster1().getUsername(), this.hdfsConfig.getMaster1().getPassword());
			String[] cmds = new String[]{
					"cd " + this.hdfsConfig.getHomeDir() + "/sbin",
					"./start-dfs.sh"
			};
			Result res = execute.execute(cmds);
			if(!res.isSuccess){
				throw new Exception("execute start hdfs service cmd failed : " + res.error_msg);
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}

	public void stop() throws Exception {
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(this.hdfsConfig.getMaster1().getIp(), this.hdfsConfig.getMaster1().getSshPort(), this.hdfsConfig.getMaster1().getUsername(), this.hdfsConfig.getMaster1().getPassword());
			String[] cmds = new String[]{
					"cd " + this.hdfsConfig.getHomeDir() + "/sbin",
					"./stop-dfs.sh"
			};
			Result res = execute.execute(cmds);
			if(!res.isSuccess){
				throw new Exception("execute stop hdfs service cmd failed : " + res.error_msg);
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}

	public String getName() {
		return "HDFS";
	}
}

