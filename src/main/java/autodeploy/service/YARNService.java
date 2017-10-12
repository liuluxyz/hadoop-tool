package autodeploy.service;

import autodeploy.config.DeployConfig;
import autodeploy.service.config.yarn.YARNConfig;
import net.neoremind.sshxcute.core.Result;

import common.linux.ExecuteLinuxSSHCMD;

/**
 * liulu5
 * 2014-7-2
 */
public class YARNService implements Service {

	YARNConfig yarnConfig;
	public YARNService(YARNConfig yarnConfig){
		this.yarnConfig = yarnConfig;
	}
	
	@Override
	public void start() throws Exception {
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(this.yarnConfig.getMaster().getIp(), this.yarnConfig.getMaster().getSshPort(), this.yarnConfig.getMaster().getUsername(), this.yarnConfig.getMaster().getPassword());
			String[] cmds = new String[]{
					"cd " + this.yarnConfig.getHomeDir() + "/sbin",
					"./start-yarn.sh"
			};
			Result res = execute.execute(cmds);
			if(!res.isSuccess){
				throw new Exception("execute start yarn service cmd failed : " + res.error_msg);
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}

	@Override
	public void stop() throws Exception {

		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(this.yarnConfig.getMaster().getIp(), this.yarnConfig.getMaster().getSshPort(), this.yarnConfig.getMaster().getUsername(), this.yarnConfig.getMaster().getPassword());
			String[] cmds = new String[]{
					"cd " + this.yarnConfig.getHomeDir() + "/sbin",
					"./stop-yarn.sh"
			};
			Result res = execute.execute(cmds);
			if(!res.isSuccess){
				throw new Exception("execute start yarn service cmd failed : " + res.error_msg);
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}

	@Override
	public String getName() {
		return "mr1";
	}
}

