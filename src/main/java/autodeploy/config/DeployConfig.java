package autodeploy.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.linux.LinuxHelper;

import autodeploy.common.ServiceEnum;
import autodeploy.common.host.Host;
import autodeploy.service.config.zookeeper.ZookeeperConfig;

/**
 * liulu5
 * 2014-5-13
 */
public class DeployConfig {

	private static final Log log = LogFactory.getLog(DeployConfig.class);

	//集群的部署方式
	private boolean hdfsDistributed;//hdfs是否部署分布式
	private boolean mr1Distributed;//mr1是否部署分布式
	private boolean yarnDistributed;//yarn是否部署分布式
	private boolean zookeeperDistributed;//zookeeper是否部署分布式
	
	private boolean namenodeHA;//是否部署namenode的ha
	private boolean namenodeFederation;//是否部署namenode的federation
	private boolean jobtrackerHA;//是否部署jobtracker的ha
	private boolean resourcemanagerHA;//是否部署resourcemanager的ha

	private boolean serviceStart;//部署后是否启动服务
	
	//hadoop cluster info
	private Host[] master;
	private Host[] slave;
	private Host[] nodes;
	
	//package info
	private String packagePath;
	private String hadoopVersion;
	
	//deploy info
	private String deployPath;
	private boolean deploySimpleDir;
	private ServiceEnum[] deployService;
	private boolean deployUseradd;
	private boolean deploySSHTrust;
	
	//若在工具中部署jdk，则java home使用部署后的java,否则使用此java home配置
	private String javaHome;
	
	private Map<String, String> hdfsConfig = new LinkedHashMap<String, String>();//hdfs相关的参数配置项
	private Map<String, String> mr1Config = new LinkedHashMap<String, String>();//mr1相关的参数配置项
	private Map<String, String> yarnConfig = new LinkedHashMap<String, String>();//yarn相关的参数配置项
	
	private String zookeeperDataDir;
	private int zookeeperClientPort;
	private Host zookeeperServer;//单机版zookeeper只配置一个server的ip
	List<ZookeeperConfig.Server> zookeeperServers = new ArrayList<ZookeeperConfig.Server>();//分布式zookeeper配置多个server
	
	private String journalnode;
	
	private DeployConfig(){
		
	}
	
	public static DeployConfig getInstance() throws Exception{
		DeployConfig config = new DeployConfig();
		config.init();
		if(!DeployConfigDetection.isValid(config)){
			throw new Exception("配置内容不合法");
		}
		config.parseHostname();
		return config;
	}
	
	private void init() throws Exception{
		InputStream stream = null;
		try {
			stream = new FileInputStream("D:\\workspace\\workspace_tools\\hadoop_tool\\src\\autodeploy\\config\\deploy.properties");
			Properties pro = new Properties();
			pro.load(stream);
			
			String rootpwd = (String) pro.get("rootpwd");
			String username = (String) pro.get("username");
			String password = (String) pro.get("password");
			
			int sshPort = Integer.parseInt(pro.get("ssh.port").toString());
			
			hdfsDistributed = Boolean.parseBoolean(pro.get("hdfs.distributed").toString());
			mr1Distributed = Boolean.parseBoolean(pro.get("mr1.distributed").toString());
			yarnDistributed = Boolean.parseBoolean(pro.get("yarn.distributed").toString());
			zookeeperDistributed = Boolean.parseBoolean(pro.get("zookeeper.distributed").toString());
					
			namenodeHA = Boolean.parseBoolean(pro.get("namenode.ha").toString());
			namenodeFederation = Boolean.parseBoolean(pro.get("namenode.federation").toString());
			jobtrackerHA = Boolean.parseBoolean(pro.get("jobtracker.ha").toString());
			resourcemanagerHA = Boolean.parseBoolean(pro.get("resourcemanager.ha").toString());
			
			serviceStart = Boolean.parseBoolean(pro.get("service.start").toString());
			
			String masterStr = (String) pro.get("master");
			if(masterStr != null){
				String[] masterIp = masterStr.split(",");
				master = new Host[masterIp.length];
				for(int i=0; i<masterIp.length; i++){
					master[i] = new Host(masterIp[i].trim(), null, sshPort, rootpwd, username, password);
				}
			}
			String slaveStr = (String) pro.get("slave");
			if(slaveStr != null){
				String[] slaveIp = slaveStr.split(",");
				slave = new Host[slaveIp.length];
				for(int i=0; i<slaveIp.length; i++){
					slave[i] = new Host(slaveIp[i].trim(), null, sshPort, rootpwd, username, password);
				}
			}
			
			List<Host> nodeList = new ArrayList<Host>();
			nodeList.addAll(Arrays.asList(slave));
			for(Host ma : master){
				if(!nodeList.contains(ma)){
					nodeList.add(ma);
				}
			}
			nodes = nodeList.toArray(new Host[0]);
			
			String deployServiceStr = (String) pro.get("deploy.service");
			if(deployServiceStr != null){
				String[] services = deployServiceStr.split(",");
				deployService = new ServiceEnum[services.length];
				for(int i=0; i<services.length; i++){
					deployService[i] = ServiceEnum.valueOf(services[i].trim());
				}
			}
			
			this.javaHome = (String) pro.get("java.home");//先从配置文件中获取此配置，后面如果部署了java，则重新设置此值
			
			packagePath = (String) pro.get("package.path");
			hadoopVersion = (String) pro.get("hadoop.version");
			deployPath = (String) pro.get("deploy.path");
			deploySimpleDir = Boolean.parseBoolean(pro.get("deploy.simpledir").toString());
			deployUseradd = Boolean.parseBoolean(pro.get("deploy.useradd").toString());
			deploySSHTrust = Boolean.parseBoolean(pro.get("deploy.sshtrust").toString());
			
			zookeeperDataDir = (String) pro.get("zookeeper.data.dir");
			zookeeperClientPort = Integer.parseInt(pro.get("zookeeper.client.port").toString());
			zookeeperServer = new Host((String) pro.get("zookeeper.server"), null, sshPort, rootpwd, username, password);
			
			journalnode = (String) pro.get("journalnode");
			
			Iterator it = pro.keySet().iterator();
			while(it.hasNext()){
				String key = it.next().toString();
				if(key.startsWith("config.hdfs.")){//hdfs配置项
					hdfsConfig.put(key.substring("config.hdfs.".length()), pro.getProperty(key));
				}
				else if(key.startsWith("config.mr1.")){//mr1配置项
					mr1Config.put(key.substring("config.mr1.".length()), pro.getProperty(key));
				}
				else if(key.startsWith("config.yarn.")){//yarn配置项
					yarnConfig.put(key.substring("config.yarn.".length()), pro.getProperty(key));
				}
				else if(key.startsWith("zookeeper.server.")){//zookeeper配置项
					int no = Integer.parseInt(key.substring(key.lastIndexOf(".")+1));
					String value = pro.getProperty(key).trim();
					String[] values = value.split(":");
					if(values.length != 3){
						throw new Exception("zookeeper server num is not 3");
					}
					Host host = new Host(values[0].trim(), null, sshPort, rootpwd, username, password);
					int connectPort = Integer.parseInt(values[1]);
					int electPort = Integer.parseInt(values[2]);
					
					ZookeeperConfig.Server server = new ZookeeperConfig.Server(no, host, connectPort, electPort);
					zookeeperServers.add(server);
				}
			}
			
		}finally{
			try {
				if(stream != null)
					stream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 解析hostname
	 * @throws Exception
	 */
	private void parseHostname()throws Exception {
		try{
			if(this.isDeployUseradd()){
				for(Host master : this.getMaster()){
					String hostname = LinuxHelper.parseHostname(master.getIp(), master.getSshPort(), "root", master.getRootpwd());
					master.setHostname(hostname);
				}
				for(Host slave : this.getSlave()){
					String hostname = LinuxHelper.parseHostname(slave.getIp(), slave.getSshPort(), "root", slave.getRootpwd());
					slave.setHostname(hostname);
				}
				if(isDeployZookeeper()){
					if(this.isZookeeperDistributed()){
						for(ZookeeperConfig.Server server : this.getZookeeperServers()){
							String hostname = LinuxHelper.parseHostname(server.getHost().getIp(), server.getHost().getSshPort(), "root", server.getHost().getRootpwd());
							server.getHost().setHostname(hostname);
						}
					}else{
						String hostname = LinuxHelper.parseHostname(this.getZookeeperServer().getIp(), this.getZookeeperServer().getSshPort(), "root", this.getZookeeperServer().getRootpwd());
						this.getZookeeperServer().setHostname(hostname);
					}
				}
			}else{
				for(Host master : this.getMaster()){
					String hostname = LinuxHelper.parseHostname(master.getIp(), master.getSshPort(), master.getUsername(), master.getPassword());
					master.setHostname(hostname);
				}
				for(Host slave : this.getSlave()){
					String hostname = LinuxHelper.parseHostname(slave.getIp(), slave.getSshPort(), slave.getUsername(), slave.getPassword());
					slave.setHostname(hostname);
				}
				if(isDeployZookeeper()){
					if(this.isZookeeperDistributed()){
						for(ZookeeperConfig.Server server : this.getZookeeperServers()){
							String hostname = LinuxHelper.parseHostname(server.getHost().getIp(), server.getHost().getSshPort(), server.getHost().getUsername(), server.getHost().getPassword());
							server.getHost().setHostname(hostname);
						}
					}else{
						String hostname = LinuxHelper.parseHostname(this.getZookeeperServer().getIp(), this.getZookeeperServer().getSshPort(), this.getZookeeperServer().getUsername(), this.getZookeeperServer().getPassword());
						this.getZookeeperServer().setHostname(hostname);
					}
				}
			}
		}catch(Exception e){
			log.error("解析hostname失败" + e);
			throw e;
		}
	}
	
//	public String getRootpwd() {
//		return rootpwd;
//	}
//	public String getUsername() {
//		return username;
//	}
//	public String getPassword() {
//		return password;
//	}
	public Host[] getMaster() {
		return master;
	}
	public Host[] getSlave() {
		return slave;
	}
	public Host[] getNodes() {
		return nodes;
	}
	public String getDeployPath() {
		return deployPath;
	}
	public boolean isDeploySimpleDir() {
		return deploySimpleDir;
	}

	//	public String getJdkDeploy() {
//		return jdkDeploy;
//	}
//	public String getJdkVersion() {
//		return jdkVersion;
//	}
	public ServiceEnum[] getDeployService() {
		return deployService;
	}
	public String getPackagePath() {
		return packagePath;
	}
	public boolean isDeployUseradd() {
		return deployUseradd;
	}
	public boolean isDeploySSHTrust() {
		return deploySSHTrust;
	}
	public boolean isHdfsDistributed() {
		return hdfsDistributed;
	}
	public boolean isMr1Distributed() {
		return mr1Distributed;
	}
	public boolean isYarnDistributed() {
		return yarnDistributed;
	}
	public boolean isZookeeperDistributed() {
		return zookeeperDistributed;
	}
	public boolean isNamenodeHA() {
		return namenodeHA;
	}
	public boolean isNamenodeFederation() {
		return namenodeFederation;
	}
	public boolean isJobtrackerHA() {
		return jobtrackerHA;
	}
	public boolean isResourcemanagerHA() {
		return resourcemanagerHA;
	}
	public boolean isServiceStart() {
		return serviceStart;
	}
	public String getJavaHome() {
		return javaHome;
	}
	public void setJavaHome(String javaHome) {
		this.javaHome = javaHome;
	}
	public Map<String, String> getHdfsConfig() {
		return hdfsConfig;
	}
	public String getZookeeperDataDir() {
		return zookeeperDataDir;
	}
	public int getZookeeperClientPort() {
		return zookeeperClientPort;
	}
	public List<ZookeeperConfig.Server> getZookeeperServers() {
		return zookeeperServers;
	}
	public Host getZookeeperServer() {
		return zookeeperServer;
	}
	public Map<String, String> getMr1Config() {
		return mr1Config;
	}
	public Map<String, String> getYarnConfig() {
		return yarnConfig;
	}
	public String getHadoopVersion() {
		return hadoopVersion;
	}
	public String getJournalnode() {
		return journalnode;
	}
	
	private boolean isDeployZookeeper(){
		for(ServiceEnum service : deployService){
			if(ServiceEnum.zookeeper.ordinal() == service.ordinal()){
				return true;
			}
		}
		return false;
	}
}

