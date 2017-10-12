package autodeploy.pack;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import common.file.GZipFileFormater;
import common.file.ZipFileFormater;

import autodeploy.common.ServiceEnum;
import autodeploy.config.DeployConfig;

/**
 * liulu5
 * 2014-5-28
 */
public class FetchPackage {

	private static final Map<ServiceEnum, String> deploySimpleDirs = new HashMap<ServiceEnum, String>();
	static{//服务部署时对应的简易安装目录名定义
		deploySimpleDirs.put(ServiceEnum.java, 		ServiceEnum.java.name());
		deploySimpleDirs.put(ServiceEnum.zookeeper, ServiceEnum.zookeeper.name());
		deploySimpleDirs.put(ServiceEnum.hdfs, 		"hadoop");
		deploySimpleDirs.put(ServiceEnum.mapreduce, "mr1");
		deploySimpleDirs.put(ServiceEnum.yarn, 		"hadoop");
		deploySimpleDirs.put(ServiceEnum.hive, 		ServiceEnum.hive.name());
		deploySimpleDirs.put(ServiceEnum.hbase, 	ServiceEnum.hbase.name());
		deploySimpleDirs.put(ServiceEnum.spark, 	ServiceEnum.spark.name());
	}
	
	private static final Map<ServiceEnum, String> packageKeyWords = new HashMap<ServiceEnum, String>();
	static{//服务与其安装包中的关键字的对应关系,通过关键字找包
		packageKeyWords.put(ServiceEnum.java, 		"jdk");
		packageKeyWords.put(ServiceEnum.zookeeper, 	ServiceEnum.zookeeper.name());
		packageKeyWords.put(ServiceEnum.hdfs, 		"hadoop");
		packageKeyWords.put(ServiceEnum.mapreduce, 	"mr1");
		packageKeyWords.put(ServiceEnum.yarn, 		"hadoop");
		packageKeyWords.put(ServiceEnum.hive, 		ServiceEnum.hive.name());
		packageKeyWords.put(ServiceEnum.hbase, 		ServiceEnum.hbase.name());
		packageKeyWords.put(ServiceEnum.spark, 		ServiceEnum.spark.name());
	}
	
	public static Package[] fetch(final DeployConfig config) throws Exception{
		File file = new File(config.getPackagePath());
		if(!file.exists() || !file.isDirectory()){
			throw new Exception("包目录不存在");
		}
		File[] packageFiles = file.listFiles();
		List<Package> packs = new ArrayList<Package>();
		for(ServiceEnum service : config.getDeployService()){
			if(ServiceEnum.yarn.ordinal() == service.ordinal()){//yarn不单独解析包，与hdfs同包
				continue;
			}
			
			String packageKeyWord = packageKeyWords.get(service);
			for(File packageFile : packageFiles){
				if(packageFile.isDirectory()){
					continue;
				}
				if(packageFile.getName().contains(packageKeyWord)){
					Package pack = new Package();
					pack.setService(service);
					pack.setName(packageFile.getName());
					pack.setPath(packageFile.getAbsolutePath());
					String rootDirName = parseRootDirFromPackage(packageFile.getAbsolutePath());
					pack.setRootDir(rootDirName);
					if(config.isDeploySimpleDir()){
						pack.setDeployDir(deploySimpleDirs.get(service));
					}else{
						pack.setDeployDir(rootDirName);
					}
					pack.setInstallCMD(parseInstallCMD(packageFile.getName()));
					packs.add(pack);
					break;
				}
			}
		}
		return packs.toArray(new Package[0]);
	}

	/**
	 * 解析安装包的部署命令
	 * @param packageName
	 * @return
	 */
	private static String parseInstallCMD(String packageName){
		if(packageName.endsWith("rpm")){
			return "rpm -ivh " + packageName;
		}
		if(packageName.endsWith("tar.gz")){
			return "tar -zxf " + packageName;
		}
		if(packageName.endsWith("gz")){
			return "gunzip " + packageName;
		}
		return null;
	}
	
	/**
	 * 解析压缩包中的根目录名
	 * @param packPath
	 * @return
	 * @throws Exception
	 */
	private static String parseRootDirFromPackage(String packPath) throws Exception{
		try{
			String[] dirs = null;
			if(packPath.endsWith("tar.gz") || packPath.endsWith("gz")){
				dirs = GZipFileFormater.parseRootDir(packPath);	
			}else if(packPath.endsWith("zip")){
				dirs = ZipFileFormater.parseRootDir(packPath);	
			}
			if(dirs == null || dirs.length == 0){
				throw new Exception("解析压缩包中的根目录名失败：无法获取根目录名 : " + packPath);
			}
			if(dirs.length > 1){
				throw new Exception("解析压缩包中的根目录名失败：根目录名个数大于1 : " + packPath);
			}
			return dirs[0];
		}catch(Exception e){
			throw e;
		}
	}
}

