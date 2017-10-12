package autodeploy.config;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import autodeploy.common.ServiceEnum;
import autodeploy.common.host.Host;

import common.linux.ExecuteLinuxSSHCMD;

/**
 * liulu5
 * 2014-5-28
 */
public class DeployConfigDetection {

	private static final Log log = LogFactory.getLog(DeployConfigDetection.class);
	
	public static boolean isValid(DeployConfig config){
		return isUserinfoValid(config)
				&& isPackageValid(config);
	}
	
	/**
	 * 检查用户信息是否正确
	 * @param config
	 * @return
	 */
	private static boolean isUserinfoValid(DeployConfig config){
		try{
			if(config.isDeployUseradd()){
				for(Host node : config.getNodes()){
					ExecuteLinuxSSHCMD ssh = new ExecuteLinuxSSHCMD(node.getIp(), node.getSshPort(), "root", node.getRootpwd());
					ssh.close();
				}
			}else{
				for(Host node : config.getNodes()){
					ExecuteLinuxSSHCMD ssh = new ExecuteLinuxSSHCMD(node.getIp(), node.getSshPort(), node.getUsername(), node.getPassword());
					ssh.close();
				}
			}
		}catch(Exception e){
			e.printStackTrace();
			log.error("用户信息不合法");
			return false;
		}
		return true;
	}
	
	/**
	 * 检查安装包配置是否正确
	 * @param config
	 * @return
	 */
	private static boolean isPackageValid(DeployConfig config){
		File file = new File(config.getPackagePath());
		if(!file.exists() || !file.isDirectory()){
			log.error("指定安装包目录不存在 : " + config.getPackagePath());
			return false;
		}
		File[] packageFiles = file.listFiles();
		for(ServiceEnum service : config.getDeployService()){
			boolean flag = false;
			for(File packageFile : packageFiles){
				String serviceName = service.name();
				if(ServiceEnum.hdfs.ordinal() == service.ordinal() 
						|| ServiceEnum.mapreduce.ordinal() == service.ordinal() 
						|| ServiceEnum.yarn.ordinal() == service.ordinal()){
					serviceName = "hadoop";
				}else if(ServiceEnum.java.ordinal() == service.ordinal()){
					serviceName = "jdk";
				}
				if(packageFile.getName().contains(serviceName)){
					flag = true;
					break;
				}
			}
			if(flag == false){
				log.error("无法找到与服务 : " + service + "对应的安装包");
				return false;
			}
		}
		return true;
	}
}

