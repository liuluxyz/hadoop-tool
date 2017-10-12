package autodeploy.model.impl.ha;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.neoremind.sshxcute.core.Result;

import common.linux.ExecuteLinuxSSHCMD;
import common.linux.LinuxHelper;

import autodeploy.common.ServiceEnum;
import autodeploy.common.host.Host;
import autodeploy.config.DeployConfig;
import autodeploy.model.DistributedModel;
import autodeploy.service.HDFSService;
import autodeploy.service.config.UpdateConfigFile;
import autodeploy.service.config.hdfs.HDFSConfig;
import autodeploy.service.config.zookeeper.ZookeeperConfig;

/**
 * @Description: HDFS带HA(QJM)的分布式
 * @author liulu5
 * @date 2014-10-3 下午11:39:03
 */
public class HDFSHADistributedModel implements DistributedModel{

	private static final Log log = LogFactory.getLog(HDFSHADistributedModel.class);
	
	public HDFSService hdfsService;
	public HDFSConfig hdfsConfig;
	
	public void parseService(DeployConfig config, Map<ServiceEnum, String> serviceHome) throws Exception {
		
		hdfsConfig = new HDFSConfig();
		hdfsConfig.setHomeDir(serviceHome.get(ServiceEnum.hdfs));
		hdfsConfig.setMaster1(config.getMaster()[0]);
		hdfsConfig.setMaster2(config.getMaster()[1]);
		hdfsConfig.setSlaves(config.getSlave());
		hdfsConfig.setJavaHome(config.getJavaHome());
		
		String nameservices = config.getHdfsConfig().remove("dfs.nameservices");
		nameservices = nameservices == null ? "mycluster" : nameservices;
		hdfsConfig.addHdfsSite("dfs.nameservices", nameservices);
		
		String namenodeStr = config.getHdfsConfig().remove("dfs.ha.namenodes." + nameservices);
		String[] namenodes = namenodeStr == null ? new String[]{"nn1", "nn2"} : namenodeStr.split(",");
		hdfsConfig.addHdfsSite("dfs.ha.namenodes." + nameservices, namenodes[0] + "," + namenodes[1]);
		
		if(!config.getHdfsConfig().containsKey("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0])){
			hdfsConfig.addHdfsSite(
					"dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], hdfsConfig.getMaster1().getHostname() + ":8020");
		}
		if(!config.getHdfsConfig().containsKey("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1])){
			hdfsConfig.addHdfsSite(
					"dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], hdfsConfig.getMaster2().getHostname() + ":8020");
		}
		if(!config.getHdfsConfig().containsKey("dfs.namenode.http-address." + nameservices + "." + namenodes[0])){
			hdfsConfig.addHdfsSite(
					"dfs.namenode.http-address." + nameservices + "." + namenodes[0], hdfsConfig.getMaster1().getHostname() + ":50070");
		}
		if(!config.getHdfsConfig().containsKey("dfs.namenode.http-address." + nameservices + "." + namenodes[1])){
			hdfsConfig.addHdfsSite(
					"dfs.namenode.http-address." + nameservices + "." + namenodes[1], hdfsConfig.getMaster2().getHostname() + ":50070");
		}
		hdfsConfig.addHdfsSite("dfs.namenode.shared.edits.dir", "qjournal://" + config.getJournalnode() + "/" + nameservices);
		
		String zooQuorum = "";
		for(ZookeeperConfig.Server zooServer : config.getZookeeperServers()){
			zooQuorum += zooServer.getHost().getHostname() + ":" + config.getZookeeperClientPort() + ",";
		}
		hdfsConfig.addHdfsSite("ha.zookeeper.quorum", zooQuorum.substring(0, zooQuorum.length()-1));
		hdfsConfig.addHdfsSite("dfs.ha.automatic-failover.enabled", "true");
		hdfsConfig.addHdfsSite("dfs.ha.fencing.methods", "shell(/bin/true)");
		hdfsConfig.addHdfsSite("dfs.permissions.enabled", "false");
		hdfsConfig.addHdfsSite("dfs.permissions", "false");
		
		hdfsConfig.addHdfsSite(config.getHdfsConfig());
		
		hdfsConfig.addCoreSite("fs.defaultFS", "hdfs://" + nameservices);

		hdfsService = new HDFSService(hdfsConfig);
	}
	
	public void update() throws Exception {
		updateMaster();
		updateSlave();
	}

	/**
	 * 更新主节点上的文件
	 * @throws Exception
	 */
	private void updateMaster() throws Exception {
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
	
	/**
	 * 通过分发更新子节点上的文件
	 * @throws Exception
	 */
	private void updateSlave() throws Exception {
		String hdfsConfDir = this.hdfsConfig.getHomeDir() + "/etc/hadoop";
		String bashFile = "/home/" + this.hdfsConfig.getMaster1().getUsername() + "/.bash_profile";
		String hadoopEnvFile = hdfsConfDir + "/hadoop-env.sh";
		String coreSiteFile = hdfsConfDir + "/core-site.xml";
		String hdfsSiteFile = hdfsConfDir + "/hdfs-site.xml";
		String slavesFile = hdfsConfDir + "/slaves";
		
		String[] files = new String[]{bashFile, hadoopEnvFile, coreSiteFile, hdfsSiteFile, slavesFile};
		String[] cmds = new String[files.length * (this.hdfsConfig.getSlaves().length + 1)];
		int index = 0;
		for(int i=0; i<files.length; i++){
			cmds[index++] = "scp " + files[i] + " " + this.hdfsConfig.getMaster2().getIp() + ":" + files[i].substring(0, files[i].lastIndexOf("/")+1);//master2
			for(int j=0; j<this.hdfsConfig.getSlaves().length; j++){//slaves
				cmds[index++] = "scp " + files[i] + " " + this.hdfsConfig.getSlaves()[j].getIp() + ":" + files[i].substring(0, files[i].lastIndexOf("/")+1);
			}
		}
		LinuxHelper.execute(this.hdfsConfig.getMaster1(), cmds);
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
				"source /home/" + hdfsConfig.getMaster1().getUsername() + "/.bash_profile",
				"hdfs dfs -mkdir " + dirName,
				"hdfs dfs -rm -r " + dirName
		};
		LinuxHelper.execute(hdfsConfig.getMaster1(), cmds);
		log.info("check end...");
	}
	
	/**
	 * 事先创建目录
	 * @param host
	 * @param dir
	 * @throws Exception
	 */
	private void prepareDir(Host[] hosts, String... dirs) throws Exception {
		log.info("prepare dir...");
		for(Host host : hosts){
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
		}
		log.info("prepare dir end...");
	}
	
	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#prepare()
	 */
	public void prepare() throws Exception {
		log.info("prepare...");
		String[] nameDirs = hdfsConfig.getHdfsSite().get("dfs.namenode.name.dir").split(",");
		String[] dataDirs = hdfsConfig.getHdfsSite().get("dfs.datanode.data.dir").split(",");
		prepareDir(new Host[]{this.hdfsConfig.getMaster1(), this.hdfsConfig.getMaster2()}, nameDirs);
		prepareDir(this.hdfsConfig.getSlaves(), dataDirs);
		
		String[] namenode1Cmds = new String[]{
				"source /home/" + hdfsConfig.getMaster1().getUsername() + "/.bash_profile",
				this.hdfsConfig.getHomeDir() + "/sbin/hadoop-daemons.sh start journalnode",
				this.hdfsConfig.getHomeDir() + "/bin/hdfs zkfc -formatZK -force",
				this.hdfsConfig.getHomeDir() + "/bin/hdfs namenode -format " + hdfsConfig.getHdfsSite().get("dfs.nameservices") + " -force"
		};
		LinuxHelper.execute(hdfsConfig.getMaster1(), namenode1Cmds);
		
		String[] namenode2Cmds = new String[]{
				"source /home/" + hdfsConfig.getMaster2().getUsername() + "/.bash_profile",
				this.hdfsConfig.getHomeDir() + "/bin/hdfs namenode -bootstrapStandby -force"};
		LinuxHelper.execute(hdfsConfig.getMaster2(), namenode2Cmds);
		
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
		Host name1 = new Host("192.168.58.133", null, 22, null, "liulu", "liulu");
		Host name2 = new Host("192.168.58.130", null, 22, null, "liulu", "liulu");
	
		String[] namenode1Cmds = new String[]{
				"source /home/liulu/.bash_profile",
				"/home/liulu/app/hadoop/sbin/hadoop-daemons.sh start journalnode",
//				"/home/liulu/app/hadoop/bin/hdfs zkfc -formatZK -force",
				"/home/liulu/app/hadoop/bin/hdfs namenode -format ns1 -force"
		};
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(name1);
			for(String cmd : namenode1Cmds){
				Result res = execute.execute(cmd);
				if(res.isSuccess){
					System.out.println("success : " + cmd);
				}else{
					System.out.println("fail : " + cmd);
					break;
				}
			}
		}catch(Exception e){
			log.error("exception", e);
		}
		finally{
			execute.close();
		}
	}
	
	
}
