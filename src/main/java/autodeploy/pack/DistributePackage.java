package autodeploy.pack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.neoremind.sshxcute.core.Result;
import common.linux.ExecuteLinuxSSHCMD;

import autodeploy.common.ServiceEnum;
import autodeploy.common.host.Host;
import autodeploy.config.DeployConfig;
import autodeploy.service.config.zookeeper.ZookeeperConfig;

/**
 * liulu5
 * 2014-5-28
 */
public class DistributePackage {

	private static final Log log = LogFactory.getLog(DistributePackage.class);
	
	/**
	 * 向对应的节点分发对应的安装包
	 * @param config
	 * @param packs
	 * @throws Exception
	 */
	public static void distribute(DeployConfig config, Package[] packs) throws Exception{
		log.info("start distribute...");
		Map<Host, List<Package>> relation = parseDistributeRelation(config, packs);
		Iterator<Host> it = relation.keySet().iterator();
		int num = 1;
		while(it.hasNext()){
			Host host = it.next();
			List<Package> packages = relation.get(host);
			ExecuteLinuxSSHCMD execute = null;
			try{
				execute = new ExecuteLinuxSSHCMD(host);
				Result res = execute.execute("cd " + config.getDeployPath());
				if(!res.isSuccess){
					res = execute.execute("mkdir -p " + config.getDeployPath());
					if(!res.isSuccess){
						throw new Exception("创建部署目录失败 : " + res.error_msg);
					}
				}
				for(int j=0; j<packages.size(); j++){
					log.info("正在为第" + num + "个节点分发第" + (j+1) + "个包," + "总共" + (relation.size()) + "个节点" + (packages.size()) + "个包");
					execute.uploadSingleDataToServer(packages.get(j).getPath(), config.getDeployPath());
					String[] cmds;
					if(config.isDeploySimpleDir()){
						cmds = new String[]{
								"cd " + config.getDeployPath(),
								packages.get(j).getInstallCMD(),
								"mv " + packages.get(j).getRootDir() + " " + packages.get(j).getDeployDir(),
								"rm " + packages.get(j).getName()
						};
					}else{
						cmds = new String[]{
								"cd " + config.getDeployPath(),
								packages.get(j).getInstallCMD(),
								"rm " + packages.get(j).getName()
						};
					}
					execute.execute(cmds);
				}
				num++;
			}finally{
				if(execute != null)
					execute.close();
			}
		}
		log.info("end distribute...");
	}
	
	/**
	 * 解析每个节点需要分发哪些安装包
	 * @param config
	 * @param packs
	 * @return
	 * @throws Exception
	 */
	private static Map<Host, List<Package>> parseDistributeRelation(DeployConfig config, Package[] packs) throws Exception{
		
		List<ServiceEnum> services = Arrays.asList(config.getDeployService());
		
		Map<Host, List<Package>> relation = new HashMap<Host, List<Package>>();
		for(Package pack : packs){
			if(ServiceEnum.java.ordinal() == pack.getService().ordinal()){
				if(services.contains(ServiceEnum.hdfs) || 
						services.contains(ServiceEnum.mapreduce) || 
						services.contains(ServiceEnum.yarn)){
					updateMap(config.getMaster(), pack, relation);
					updateMap(config.getSlave(), pack, relation);
				}
				if(services.contains(ServiceEnum.zookeeper)){
					if(config.isZookeeperDistributed()){
						updateMap(config.getZookeeperServers(), pack, relation);
					}else{
						updateMap(new Host[]{config.getZookeeperServer()}, pack, relation);
					}
				}
			}
			else if(ServiceEnum.zookeeper.ordinal() == pack.getService().ordinal()){
				if(config.isZookeeperDistributed()){
					updateMap(config.getZookeeperServers(), pack, relation);
				}else{
					updateMap(new Host[]{config.getZookeeperServer()}, pack, relation);
				}
			}
			else if(ServiceEnum.hdfs.ordinal() == pack.getService().ordinal() || 
					ServiceEnum.mapreduce.ordinal() == pack.getService().ordinal() || 
					ServiceEnum.yarn.ordinal() == pack.getService().ordinal()){
				updateMap(config.getMaster(), pack, relation);
				updateMap(config.getSlave(), pack, relation);
			}
			else if(ServiceEnum.hive.ordinal() == pack.getService().ordinal()){
				//待补充
			}
			else if(ServiceEnum.hbase.ordinal() == pack.getService().ordinal()){
				//待补充
			}
			else if(ServiceEnum.spark.ordinal() == pack.getService().ordinal()){
				//待补充
			}
		}
		return relation;
	}
	
	/**
	 * 将zookeeper的包与其需要分发的节点进行组合
	 * @param servers
	 * @param pack
	 * @param relation
	 */
	private static void updateMap(List<ZookeeperConfig.Server> servers, Package pack, Map<Host, List<Package>> relation){
		Host[] hosts = new Host[servers.size()];
		for(int i=0; i<servers.size(); i++){
			hosts[i] = servers.get(i).getHost();
		}
		updateMap(hosts, pack, relation);
	}
	
	/**
	 * 将包与其需要分发的节点进行组合
	 * @param hosts
	 * @param pack
	 * @param relation
	 */
	private static void updateMap(Host[] hosts, Package pack, Map<Host, List<Package>> relation){
		for(Host host : hosts){
			Iterator<Host> it = relation.keySet().iterator();
			boolean isExist = false;
			while(it.hasNext()){
				Host hostInRelation = it.next();
				if(hostInRelation.equals(host)){
					if(!relation.get(hostInRelation).contains(pack)){
						relation.get(hostInRelation).add(pack);
					}
					isExist = true;
					break;
				}
			}
			if(isExist == false){
				List<Package> temp = new ArrayList<Package>();
				temp.add(pack);
				relation.put(host, temp);
			}
		}
	}
	
	/**
	 * 从安装包对象中解析每个服务的home目录
	 * @param config
	 * @param packs
	 * @return
	 * @throws Exception
	 */
	public static Map<ServiceEnum, String> parseHome(DeployConfig config, Package[] packs) throws Exception{
		Map<ServiceEnum, String> serviceHome = new HashMap<ServiceEnum, String>();
		for(int j=0; j<packs.length; j++){
			String homeDir = config.getDeployPath().endsWith("/") ? (config.getDeployPath() + packs[j].getDeployDir()) : (config.getDeployPath() + "/" + packs[j].getDeployDir());
			serviceHome.put(packs[j].getService(), homeDir);
		}
		return serviceHome;
	}
}

