package autodeploy.service;

import autodeploy.service.config.mr1.MR1Config;
import net.neoremind.sshxcute.core.Result;

import common.linux.ExecuteLinuxSSHCMD;

/**
 * liulu5
 * 2014-7-2
 */
public class MR1Service implements Service {
	
	MR1Config mr1Config;
	public MR1Service(MR1Config mr1Config){
		this.mr1Config = mr1Config;
	}
	
	@Override
	public void start() throws Exception {
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(this.mr1Config.getMaster().getIp(), this.mr1Config.getMaster().getSshPort(), this.mr1Config.getMaster().getUsername(), this.mr1Config.getMaster().getPassword());
			String[] cmds = new String[]{
					"cd " + this.mr1Config.getHomeDir() + "/bin",
					"./start-mapred.sh"
			};
			Result res = execute.execute(cmds);
			if(!res.isSuccess){
				throw new Exception("execute start mr1 service cmd failed : " + res.error_msg);
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
			execute = new ExecuteLinuxSSHCMD(this.mr1Config.getMaster().getIp(), this.mr1Config.getMaster().getSshPort(), this.mr1Config.getMaster().getUsername(), this.mr1Config.getMaster().getPassword());
			String[] cmds = new String[]{
					"cd " + this.mr1Config.getHomeDir() + "/bin",
					"./stop-mapred.sh"
			};
			Result res = execute.execute(cmds);
			if(!res.isSuccess){
				throw new Exception("execute start mr1 service cmd failed : " + res.error_msg);
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

