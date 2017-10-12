package autodeploy;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import autodeploy.common.ServiceEnum;
import autodeploy.config.DeployConfig;
import autodeploy.createSSH.CreateSSHTrust;
import autodeploy.createuser.CreateUser;
import autodeploy.model.DistributedModel;
import autodeploy.model.DistributedModelFactory;
import autodeploy.pack.DistributePackage;
import autodeploy.pack.FetchPackage;
import autodeploy.pack.Package;

/**
 * liulu5
 * 2014-5-6
 */
public class AutoDeployHadoop {

	private static final Log log = LogFactory.getLog(AutoDeployHadoop.class);
	
	DeployConfig config;
	
	public void init() throws Exception{
		config = DeployConfig.getInstance();
	}
	
	public void start() throws Exception{
		log.info("start to deploy...");
		
		//1. 创建用户
		if(config.isDeployUseradd()){
			//1. create user
			CreateUser createUser = new CreateUser(config.getNodes());
			createUser.startCreate();
			log.info("创建用户成功");
		}
		
		//2. 建立互信
		if(config.isDeploySSHTrust()){
			//1. create ssh trust
			CreateSSHTrust createSSHTrust = new CreateSSHTrust();
			createSSHTrust.set(config.getNodes());
			createSSHTrust.startCreate();
			log.info("建立互信成功");
		}
		
		//3. 获取安装包
		Package[] packs = FetchPackage.fetch(config);
		log.info("获取安装包个数 : " + packs.length);
		
		//4. 解析出服务对应的安装目录
		Map<ServiceEnum, String> serviceHome = DistributePackage.parseHome(config, packs);
		if(serviceHome.containsKey(ServiceEnum.java)){//若部署了java，则重新设置config中的javahome
			config.setJavaHome(serviceHome.get(ServiceEnum.java));
		}
		//5. 分发、安装服务包
		DistributePackage.distribute(config, packs);
		
		//6. 解析服务部署模式，并逐个部署
		DistributedModel[] models = DistributedModelFactory.parseDistributedModel(config);
		for(DistributedModel model : models){
			//6.1. 解析服务配置参数
			model.parseService(config, serviceHome);
			
			//6.2. 修改服务配置参数
			model.update();
			
			//6.3. 启动服务前的准备操作
			model.prepare();
			
			if(config.isServiceStart()){
				//6.4. 启动服务
				model.start();
				
				//6.5. 检查服务
				model.check();
			}
		}
		
		log.info("end to deploy...");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
			AutoDeployHadoop deploy = new AutoDeployHadoop();
			deploy.init();
			deploy.start();
		} catch (Exception e) {
			log.error("error : ", e);
		}
	}
}

