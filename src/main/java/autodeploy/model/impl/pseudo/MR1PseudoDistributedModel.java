package autodeploy.model.impl.pseudo;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.linux.LinuxHelper;

import autodeploy.common.ServiceEnum;
import autodeploy.config.DeployConfig;
import autodeploy.model.DistributedModel;
import autodeploy.service.MR1Service;
import autodeploy.service.config.UpdateConfigFile;
import autodeploy.service.config.mr1.MR1Config;

/**
 * MR1伪分布式
 * liulu5
 * 2014-7-2
 */
public class MR1PseudoDistributedModel implements DistributedModel{

	private static final Log log = LogFactory.getLog(MR1PseudoDistributedModel.class);
	
	public MR1Service mr1Service;
	public MR1Config mr1Config;
	
	public void parseService(DeployConfig config, Map<ServiceEnum, String> serviceHome) throws Exception {
		
		mr1Config = new MR1Config();
		mr1Config.setHomeDir(serviceHome.get(ServiceEnum.mapreduce));
		mr1Config.setMaster(config.getMaster()[0]);
		mr1Config.setSlaves(config.getSlave());
		mr1Config.setJavaHome(config.getJavaHome());
		mr1Config.addMapredSite("mapred.job.tracker", config.getMaster()[0].getHostname() + ":9001");
		mr1Config.addMapredSite("mapred.temp.dir", "${hadoop.tmp.dir}/mapred/temp");
//		mr1Config.addHadoopEnv("JAVA_HOME", mr1Config.getJavaHome());
		
		mr1Service = new MR1Service(mr1Config);
	}
	
	public void update() throws Exception {
		//update HADOOP_HOME in .bash_profile
		UpdateConfigFile.updateHadoopHomeInBashProfile(this.mr1Config.getMaster(), this.mr1Config.getHomeDir());
		
		//mr1: update JAVA_HOME in hadoop_env.sh
		String mr1ConfDir = this.mr1Config.getHomeDir() + "/conf";
		UpdateConfigFile.updateJavaHomeInHadoopEnv(this.mr1Config.getMaster(), mr1ConfDir, this.mr1Config.getJavaHome());
		
		//mr1: update HADOOP_SSH_OPTS in hadoop_env.sh
		if(this.mr1Config.getMaster().getSshPort() != 22){
			String sshOpts = "-p " + this.mr1Config.getMaster().getSshPort();
			UpdateConfigFile.updateSSHOptsInHadoopEnv(this.mr1Config.getMaster(), mr1ConfDir, sshOpts);	
		}
		
		//mr1: update mapred-site.xml
		String mapredSiteFile = mr1ConfDir + "/mapred-site.xml";
		UpdateConfigFile.updateXml(this.mr1Config.getMaster(), mapredSiteFile, this.mr1Config.getMapredSite());
				
		//mr1: copy hdfs site file to mr1 conf
		String getHomeCmd = "source /home/" + this.mr1Config.getMaster().getUsername() + "/.bash_profile";
		String copyCoreSiteCmd = "cp $HADOOP_HDFS_HOME/etc/hadoop/core-site.xml " + mr1ConfDir;
		String copyHdfsSiteCmd = "cp $HADOOP_HDFS_HOME/etc/hadoop/hdfs-site.xml " + mr1ConfDir;
		LinuxHelper.execute(this.mr1Config.getMaster(), getHomeCmd, copyCoreSiteCmd, copyHdfsSiteCmd);

		//mr1: update slaves
		UpdateConfigFile.updateSlaves(this.mr1Config.getMaster(), mr1ConfDir, this.mr1Config.getSlaves());
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#start()
	 */
	public void start() throws Exception {
		log.info("start...");
		this.mr1Service.start();
		log.info("start end...");
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#check()
	 */
	public void check() throws Exception {
		log.info("check...");
		String inDir = "/in" + UUID.randomUUID();
		String outDir = "/out" + UUID.randomUUID();
		String[] cmds = new String[]{
				"source /home/" + this.mr1Config.getMaster().getUsername() + "/.bash_profile",
				"hdfs dfs -mkdir " + inDir,
				"hdfs dfs -put " + this.mr1Config.getHomeDir() + "/conf/mapred-site.xml " + inDir,
				"hadoop jar " + this.mr1Config.getHomeDir() + "/hadoop-examples*mr1*jar wordcount " + inDir + " " + outDir,
				"hdfs dfs -rm -r " + inDir,
				"hdfs dfs -rm -r " + outDir
		};
		LinuxHelper.execute(this.mr1Config.getMaster(), cmds);
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
		return ServiceEnum.mapreduce;
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
