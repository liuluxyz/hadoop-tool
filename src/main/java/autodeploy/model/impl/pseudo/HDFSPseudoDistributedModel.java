package autodeploy.model.impl.pseudo;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.neoremind.sshxcute.core.Result;

import common.linux.ExecuteLinuxSSHCMD;
import common.linux.LinuxHelper;

import autodeploy.AutoDeployHadoop;
import autodeploy.common.ServiceEnum;
import autodeploy.common.host.Host;
import autodeploy.config.DeployConfig;
import autodeploy.model.DistributedModel;
import autodeploy.service.HDFSService;
import autodeploy.service.MR1Service;
import autodeploy.service.config.UpdateConfigFile;
import autodeploy.service.config.hdfs.HDFSConfig;
import autodeploy.service.config.mr1.MR1Config;

/**
 * HDFS伪分布式
 * liulu5
 * 2014-7-2
 */
public class HDFSPseudoDistributedModel implements DistributedModel{

	private static final Log log = LogFactory.getLog(HDFSPseudoDistributedModel.class);
	
	public HDFSService hdfsService;
	public HDFSConfig hdfsConfig;
	
	public void parseService(DeployConfig config, Map<ServiceEnum, String> serviceHome) throws Exception {
		hdfsConfig = new HDFSConfig();
		hdfsConfig.setMaster1(config.getMaster()[0]);
		hdfsConfig.setSlaves(config.getSlave());
		hdfsConfig.setHomeDir(serviceHome.get(ServiceEnum.hdfs));
		hdfsConfig.setJavaHome(config.getJavaHome());
		hdfsConfig.addCoreSite("fs.defaultFS", "hdfs://" + config.getMaster()[0].getHostname());
		hdfsConfig.setHdfsSite(config.getHdfsConfig());
//		Map<String, String> configs = config.getHdfsConfig();
//		Iterator<String> it = configs.keySet().iterator();
//		while(it.hasNext()){
//			String key = it.next();
//			String value = configs.get(key);
//			hdfsConfig.addHdfsSite(key, value);
//		}
//		hdfsConfig.addHdfsSite("dfs.namenode.name.dir", config.getConfig("dfs.namenode.name.dir"));
//		hdfsConfig.addHdfsSite("dfs.datanode.data.dir", config.getConfig("dfs.datanode.data.dir"));
//		hdfsConfig.addHadoopEnv("JAVA_HOME", hdfsConfig.getJavaHome());
		
		hdfsService = new HDFSService(hdfsConfig);
	}
	
	public void update() throws Exception {
		//update JAVA_HOME in .bash_profile
		UpdateConfigFile.updateJavaHomeInBashProfile(this.hdfsConfig.getMaster1(), this.hdfsConfig.getJavaHome());

		//update HADOOP_HDFS_HOME in .bash_profile
		UpdateConfigFile.updateHadoopHdfsHomeInBashProfile(this.hdfsConfig.getMaster1(), this.hdfsConfig.getHomeDir());
		
		//hdfs: update JAVA_HOME in hadoop_env.sh
		String hdfsConfDir = this.hdfsConfig.getHomeDir() + "/etc/hadoop";
		UpdateConfigFile.updateJavaHomeInHadoopEnv(this.hdfsConfig.getMaster1(), hdfsConfDir, this.hdfsConfig.getJavaHome());
		
		//hdfs: update HADOOP_SSH_OPTS in hadoop_env.sh
		if(this.hdfsConfig.getMaster1().getSshPort() != 22){
			String sshOpts = "-p " + this.hdfsConfig.getMaster1().getSshPort();
			UpdateConfigFile.updateSSHOptsInHadoopEnv(this.hdfsConfig.getMaster1(), hdfsConfDir, sshOpts);	
		}
		
		//hdfs: update core-site.xml & hdfs-site.xml
		String coreSiteFile = hdfsConfDir + "/core-site.xml";
		UpdateConfigFile.updateXml(this.hdfsConfig.getMaster1(), coreSiteFile, this.hdfsConfig.getCoreSite());
		String hdfsSiteFile = hdfsConfDir + "/hdfs-site.xml";
		UpdateConfigFile.updateXml(this.hdfsConfig.getMaster1(), hdfsSiteFile, this.hdfsConfig.getHdfsSite());
		
		//hdfs: update slaves
		UpdateConfigFile.updateSlaves(this.hdfsConfig.getMaster1(), hdfsConfDir, this.hdfsConfig.getSlaves());
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#start()
	 */
	public void start() throws Exception {
		log.info("start...");
		this.hdfsService.start();
		log.info("start end...");
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#check()
	 */
	public void check() throws Exception {
		log.info("check...");
		String dirName = "/temp" + UUID.randomUUID();
		String[] cmds = new String[]{
				"source /home/" + this.hdfsConfig.getMaster1().getUsername() + "/.bash_profile",
				"hdfs dfs -mkdir " + dirName,
				"hdfs dfs -rm -r " + dirName
		};
		LinuxHelper.execute(this.hdfsConfig.getMaster1(), cmds);
		log.info("check end...");
	}
	
	/**
	 * 事先创建目录
	 * @param host
	 * @param dir
	 * @throws Exception
	 */
	private void prepareDir(Host host, String... dirs) throws Exception {
		log.info("prepare dir...");
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			for(String dir : dirs){
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
			}
		}finally{
			execute.close();
		}
		log.info("prepare dir end...");
	}
	
	/**
	 * format hdfs
	 * @throws Exception
	 */
	private void format() throws Exception {
		log.info("format...");
		String[] cmds = new String[]{
				"rm -fr " + hdfsConfig.getHdfsSite().get("dfs.namenode.name.dir") + "/*", 
				"cd " + hdfsConfig.getHomeDir() + "/bin",
				"./hdfs namenode -format"
		};
		LinuxHelper.execute(this.hdfsConfig.getMaster1(), cmds);
		log.info("format end...");
	}
	
	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#prepare()
	 */
	public void prepare() throws Exception {
		log.info("prepare...");
		String[] nameDirs = hdfsConfig.getHdfsSite().get("dfs.namenode.name.dir").split(",");
		String[] dataDirs = hdfsConfig.getHdfsSite().get("dfs.datanode.data.dir").split(",");
		prepareDir(this.hdfsConfig.getMaster1(), nameDirs);
		prepareDir(this.hdfsConfig.getMaster1(), dataDirs);
		
		format();
		log.info("prepare end...");
	}
	
	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#selfService()
	 */
	public ServiceEnum selfService() {
		return ServiceEnum.hdfs;
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
	
	public static void main(String[] args) {
		Host host = new Host("192.168.58.128", null, 22, null, "liulu", "liulu");
		HDFSPseudoDistributedModel m = new HDFSPseudoDistributedModel();
		try {
			m.prepareDir(host, "/home/liulu/data/namenode");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
