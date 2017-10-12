package autodeploy.model.impl.pseudo;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import net.neoremind.sshxcute.core.Result;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.linux.ExecuteLinuxSSHCMD;
import common.linux.LinuxHelper;

import autodeploy.common.ServiceEnum;
import autodeploy.config.DeployConfig;
import autodeploy.model.DistributedModel;
import autodeploy.service.ZookeeperService;
import autodeploy.service.config.UpdateConfigFile;
import autodeploy.service.config.zookeeper.ZookeeperConfig;

/**
 * zookeeper单机版部署
 * @Description: TODO
 * @author liulu5
 * @date 2014-8-8 下午4:24:30
 */
public class ZookeeperPseudoDistributedModel implements DistributedModel{

	private static final Log log = LogFactory.getLog(ZookeeperPseudoDistributedModel.class);
	
	public ZookeeperService zookeeperService;
	public ZookeeperConfig zookeeperConfig;
	
	public void parseService(DeployConfig config, Map<ServiceEnum, String> serviceHome) throws Exception {
		zookeeperConfig = new ZookeeperConfig();
		zookeeperConfig.setZookeeperDistributed(false);
		zookeeperConfig.setHomeDir(serviceHome.get(ServiceEnum.zookeeper));
		zookeeperConfig.setDataDir(config.getZookeeperDataDir());
		zookeeperConfig.setClientPort(config.getZookeeperClientPort());
		zookeeperConfig.setServer(config.getZookeeperServer());

		zookeeperService = new ZookeeperService(zookeeperConfig);
	}
	
	public void update() throws Exception {
		String zookeeperConfDir = this.zookeeperConfig.getHomeDir() + "/conf";
		
		//prepare zoo.cfg
		LinuxHelper.execute(this.zookeeperConfig.getServer(), "cp " + zookeeperConfDir + "/zoo_sample.cfg " + zookeeperConfDir + "/zoo.cfg");
		
		Map<String, String> configs = new LinkedHashMap<String, String>();
		configs.put("dataDir", zookeeperConfig.getDataDir());
		configs.put("clientPort", zookeeperConfig.getClientPort()+"");
		
		UpdateConfigFile.updateZookeeperConfig(this.zookeeperConfig.getServer(), zookeeperConfDir, configs);	
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#start()
	 */
	public void start() throws Exception {
		log.info("start...");
		this.zookeeperService.start();
		log.info("start end...");
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#check()
	 */
	public void check() throws Exception {
		log.info("check...");
		UUID uuid = UUID.randomUUID();
		String path = "/test" + uuid.toString();
		String data = uuid.toString();
		String createCmd = this.zookeeperConfig.getHomeDir() + "/bin/zkCli.sh -server 127.0.0.1:" + zookeeperConfig.getClientPort() + " create " + path + " " + data;
		String getCmd = this.zookeeperConfig.getHomeDir() + "/bin/zkCli.sh -server 127.0.0.1:" + zookeeperConfig.getClientPort() + " get " + path;
		String deleteCmd = this.zookeeperConfig.getHomeDir() + "/bin/zkCli.sh -server 127.0.0.1:" + zookeeperConfig.getClientPort() + " delete " + path;
		
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(this.zookeeperConfig.getServer());
			execute.execute(createCmd);
			
			Result res = execute.execute(getCmd);
			if(!res.sysout.trim().endsWith(data)){
				throw new Exception("fail to create check zookeeper : " + res.error_msg + " " + res.sysout);
			}
			execute.execute(deleteCmd);
		
		}catch(Exception e){
			log.error("check zookeeper fail", e);
			throw e;
		}
		finally{
			execute.close();
		}
		log.info("check end...");
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#prepare()
	 */
	public void prepare() throws Exception {
		log.info("no prepare...");
	}
	
	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#selfService()
	 */
	public ServiceEnum selfService() {
		return ServiceEnum.zookeeper;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Object o) {
		if(!(o instanceof DistributedModel)){
			return -1;
		}
		return this.selfService().ordinal() - ((DistributedModel)o).selfService().ordinal();
	}
}
