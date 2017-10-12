package autodeploy.model.impl.nonha;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import net.neoremind.sshxcute.core.Result;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.linux.ExecuteLinuxSSHCMD;
import common.linux.LinuxHelper;

import autodeploy.common.ServiceEnum;
import autodeploy.common.host.Host;
import autodeploy.config.DeployConfig;
import autodeploy.model.DistributedModel;
import autodeploy.service.ZookeeperService;
import autodeploy.service.config.UpdateConfigFile;
import autodeploy.service.config.zookeeper.ZookeeperConfig;
import autodeploy.service.config.zookeeper.ZookeeperConfig.Server;

/**
 * zookeeper分布式部署
 * @Description: TODO
 * @author liulu5
 * @date 2014-8-11 下午2:33:12
 */
public class ZookeeperDistributedModel implements DistributedModel{

	private static final Log log = LogFactory.getLog(ZookeeperDistributedModel.class);
	
	public ZookeeperService zookeeperService;
	public ZookeeperConfig zookeeperConfig;
	
	public void parseService(DeployConfig config, Map<ServiceEnum, String> serviceHome) throws Exception {
		zookeeperConfig = new ZookeeperConfig();
		zookeeperConfig.setZookeeperDistributed(true);
		zookeeperConfig.setHomeDir(serviceHome.get(ServiceEnum.zookeeper));
		zookeeperConfig.setDataDir(config.getZookeeperDataDir());
		zookeeperConfig.setClientPort(config.getZookeeperClientPort());
		
		ZookeeperConfig.Server[] servers = config.getZookeeperServers().toArray(new ZookeeperConfig.Server[0]);
		Arrays.sort(servers);
		zookeeperConfig.setServers(servers);
		
		zookeeperService = new ZookeeperService(zookeeperConfig);
	}
	
	public void update() throws Exception {
		
		Map<String, String> configs = new LinkedHashMap<String, String>();
		configs.put("dataDir", zookeeperConfig.getDataDir());
		configs.put("clientPort", zookeeperConfig.getClientPort()+"");
		for(ZookeeperConfig.Server server : zookeeperConfig.getServers()){
			configs.put("server." + server.getNo(), server.getHost().getIp() + ":" + server.getConnectPort() + ":" + server.getElectPort());
		}
		
		String zookeeperConfDir = this.zookeeperConfig.getHomeDir() + "/conf";
		for(ZookeeperConfig.Server server : zookeeperConfig.getServers()){
			//prepare zoo.cfg
			LinuxHelper.execute(server.getHost(), "cp " + zookeeperConfDir + "/zoo_sample.cfg " + zookeeperConfDir + "/zoo.cfg");
			UpdateConfigFile.updateZookeeperConfig(server.getHost(), zookeeperConfDir, configs);
			
			prepareDir(server.getHost(), zookeeperConfig.getDataDir());
			String createMyidCmd = "echo " + server.getNo() + " > " + zookeeperConfig.getDataDir() + "/myid";
			LinuxHelper.execute(server.getHost(), createMyidCmd);
		}
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#start()
	 */
	public void start() throws Exception {
		log.info("start...");
		this.zookeeperService.start();
		log.info("start end...");
	}

	/**
	 * 事先创建好目录
	 * @param host
	 * @param dir
	 * @throws Exception
	 */
	private void prepareDir(Host host, String dir) throws Exception {
		log.info("prepare dir...");
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(host);
			Result res = execute.execute("cd " + dir);
			if(!res.isSuccess){
				res = execute.execute("mkdir -p " + dir);
				if(!res.isSuccess){
					throw new Exception("create directory failed : " + dir + ", error message : " + res.error_msg);
				}
			}else{
				res = execute.execute("rm -fr " + dir + "/*");//若存在，则清空目录
				if(!res.isSuccess){
					throw new Exception("rm directory content failed : " + dir + ", error message : " + res.error_msg);
				}
			}
		}finally{
			execute.close();
		}
		log.info("prepare dir end...");
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
			execute = new ExecuteLinuxSSHCMD(this.zookeeperConfig.getServers()[0].getHost());
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
