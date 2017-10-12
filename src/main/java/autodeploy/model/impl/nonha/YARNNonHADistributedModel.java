package autodeploy.model.impl.nonha;

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
import autodeploy.service.YARNService;
import autodeploy.service.config.UpdateConfigFile;
import autodeploy.service.config.yarn.YARNConfig;

/**
 * @Description: YARN的不带HA的分布式
 * @author liulu5
 * @date 2014-7-30 下午2:30:03
 */
public class YARNNonHADistributedModel implements DistributedModel{

	private static final Log log = LogFactory.getLog(YARNNonHADistributedModel.class);
	
	public YARNService yarnService;
	public YARNConfig yarnConfig;
	
	public void parseService(DeployConfig config, Map<ServiceEnum, String> serviceHome) throws Exception {
		
		yarnConfig = new YARNConfig();
		yarnConfig.setHomeDir(serviceHome.get(ServiceEnum.hdfs));//yarn与hdfs是同一个安装目录
		yarnConfig.setMaster(config.getMaster()[0]);
		yarnConfig.setSlaves(config.getSlave());
		yarnConfig.addMapredSite("mapreduce.framework.name", "yarn");
//		yarnConfig.addMapredSite("mapred.temp.dir", "${hadoop.tmp.dir}/mapred/temp");
		if("cdh5.0.0".equals(config.getHadoopVersion())){
			yarnConfig.addYarnSite("yarn.nodemanager.aux-services", "mapreduce_shuffle");
		}else{
			yarnConfig.addYarnSite("yarn.nodemanager.aux-services", "mapreduce.shuffle");
		}
		yarnConfig.addYarnSite("yarn.nodemanager.aux-services.mapreduce.shuffle.class", "org.apache.hadoop.mapred.ShuffleHandler");
		yarnConfig.addYarnSite("yarn.resourcemanager.hostname", config.getMaster()[0].getHostname());
		yarnConfig.addYarnSite(config.getYarnConfig());
		
//		yarnConfig.addYarnSite("yarn.resourcemanager.address", "${yarn.resourcemanager.hostname}:8032");
//		yarnConfig.addYarnSite("yarn.resourcemanager.scheduler.address", "${yarn.resourcemanager.hostname}:8030");
////		yarnConfig.addYarnSite("yarn.resourcemanager.webapp.address", "${yarn.resourcemanager.hostname}:8088");
////		yarnConfig.addYarnSite("yarn.resourcemanager.webapp.https.address", "${yarn.resourcemanager.hostname}:8090");
//		yarnConfig.addYarnSite("yarn.resourcemanager.resource-tracker.address", "${yarn.resourcemanager.hostname}:8031");
////		yarnConfig.addYarnSite("yarn.resourcemanager.admin.address", "${yarn.resourcemanager.hostname}:8033");
		
		//slaves : no need. hdfs update this file
		
		yarnService = new YARNService(yarnConfig);
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
		String yarnConfDir = this.yarnConfig.getHomeDir() + "/etc/hadoop";
		
		//prepare mapred-site.xml
		LinuxHelper.execute(this.yarnConfig.getMaster(), "cp " + yarnConfDir + "/mapred-site.xml.template " + yarnConfDir + "/mapred-site.xml");
		
		//yarn: update mapred-site.xml
		String mapredSiteFile = yarnConfDir + "/mapred-site.xml";
		UpdateConfigFile.updateXml(this.yarnConfig.getMaster(), mapredSiteFile, this.yarnConfig.getMapredSite());
		
		//yarn: update yarn-site.xml
		String yarnSiteFile = yarnConfDir + "/yarn-site.xml";
		UpdateConfigFile.updateXml(this.yarnConfig.getMaster(), yarnSiteFile, this.yarnConfig.getYarnSite());
	}
	
	/**
	 * 通过分发更新子节点上的文件
	 * @throws Exception
	 */
	private void updateSlave() throws Exception {
		String yarnConfDir = this.yarnConfig.getHomeDir() + "/etc/hadoop";
		String mapredSiteFile = yarnConfDir + "/mapred-site.xml";
		String yarnSiteFile = yarnConfDir + "/yarn-site.xml";
		
		String[] files = new String[]{mapredSiteFile, yarnSiteFile};
		String[] cmds = new String[files.length * this.yarnConfig.getSlaves().length];
		int index = 0;
		for(int i=0; i<files.length; i++){
			for(int j=0; j<this.yarnConfig.getSlaves().length; j++){
				cmds[index++] = "scp " + files[i] + " " + this.yarnConfig.getSlaves()[j].getIp() + ":" + files[i].substring(0, files[i].lastIndexOf("/")+1);
			}
		}
		LinuxHelper.execute(this.yarnConfig.getMaster(), cmds);
	}
	
	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#start()
	 */
	public void start() throws Exception {
		log.info("start...");
		this.yarnService.start();
		log.info("start end...");
	}

	/* (non-Javadoc)
	 * @see autodeploy.model.DistributedModel#check()
	 */
	public void check() throws Exception {
		log.info("check...");
		
		String checkNodeNumCmd = this.yarnConfig.getHomeDir() + "/bin/yarn node -list";
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(this.yarnConfig.getMaster());
			Result res = execute.execute(checkNodeNumCmd);
			if(!res.isSuccess){
				throw new Exception("check yarn fail : " + res.error_msg);
			}
			String temp = res.sysout.substring(res.sysout.indexOf("Total Nodes"));
			int nodeNum = Integer.parseInt(temp.substring("Total Nodes:".length(), temp.indexOf("Node-Id")-1).trim());
			if(nodeNum != yarnConfig.getSlaves().length){
				throw new Exception("running node is : " + nodeNum + ", should be " + yarnConfig.getSlaves().length);
			}
		}finally{
			execute.close();
		}
		
		String inDir = "/in" + UUID.randomUUID();
		String outDir = "/out" + UUID.randomUUID();
		String[] cmds = new String[]{
				"source /home/" + this.yarnConfig.getMaster().getUsername() + "/.bash_profile",
				"hdfs dfs -mkdir " + inDir,
				"hdfs dfs -put " + this.yarnConfig.getHomeDir() + "/etc/hadoop/yarn-site.xml " + inDir,
				"yarn jar " + this.yarnConfig.getHomeDir() + "/share/hadoop/mapreduce/hadoop-mapreduce-examples*jar wordcount " + inDir + " " + outDir,
				"hdfs dfs -rm -r " + inDir,
				"hdfs dfs -rm -r " + outDir
		};
		LinuxHelper.execute(this.yarnConfig.getMaster(), cmds);
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
		return ServiceEnum.yarn;
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
