package autodeploy.service.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.neoremind.sshxcute.core.Result;

import common.linux.ExecuteLinuxSSHCMD;

import autodeploy.common.host.Host;

/**
 * liulu5
 * 2014-7-10
 */
public class UpdateConfigFile {

	private static final Log log = LogFactory.getLog(UpdateConfigFile.class);
	
	/**
	 * 更新节点中用户下的bash_profile中的JAVA_HOME
	 */
	public static void updateJavaHomeInBashProfile(Host host, String javaHome) throws Exception{
		updateBashProfile(host, "JAVA_HOME", javaHome, "$JAVA_HOME/bin");
	}
	
	/**
	 * 更新节点中用户下的bash_profile中的HADOOP_HOME
	 */
	public static void updateHadoopHomeInBashProfile(Host host, String hadoopHome) throws Exception{
		updateBashProfile(host, "HADOOP_HOME", hadoopHome, "$HADOOP_HOME/bin");
	}
	
	/**
	 * 更新节点中用户下的bash_profile中的HADOOP_HDFS_HOME
	 */
	public static void updateHadoopHdfsHomeInBashProfile(Host host, String hadoopHdfsHome) throws Exception{
		updateBashProfile(host, "HADOOP_HDFS_HOME", hadoopHdfsHome, "$HADOOP_HDFS_HOME/bin");
	}
	
	/**
	 * 更新节点中用户下的bash_profile中的HIVE_HOME
	 */
	public static void updateHiveHomeInBashProfile(Host host, String hiveHome) throws Exception{
		updateBashProfile(host, "HIVE_HOME", hiveHome, "$HIVE_HOME/bin");
	}
	
	/**
	 * 更新节点中用户下的bash_profile中的ZOOKEEPER_HOME
	 */
	public static void updateZookeeperHomeInBashProfile(Host host, String zookeeperHome) throws Exception{
		updateBashProfile(host, "ZOOKEEPER_HOME", zookeeperHome, "$ZOOKEEPER_HOME/bin");
	}
	
	/**
	 * 更新节点中用户的bash_profile中的export变量
	 */
	private static void updateBashProfile(Host host, String exportKey, String exportValue, String addIntoPathValue) throws Exception{
		if(exportKey == null || "".equals(exportKey) || exportValue == null || "".equals(exportValue)){
			throw new Exception("no export key!");
		}
		if(exportValue == null || "".equals(exportValue)){
			throw new Exception("no export value!");
		}
		
		String bashFile = "/home/" + host.getUsername() + "/.bash_profile";
		String exportLine = "export " + exportKey + "=" + exportValue.replaceAll("/", "\\\\/");
		
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			//开始添加export variable
			//1.1 获取已存在的export variable设置
			Result res = execute.execute("sed -n '/^export \\{1,\\}" + exportKey + "=*/p' " + bashFile);
			String oldExportLine = null;
			if(!res.isSuccess){
				log.error("execute sed to get " + exportKey + " from bash_profile fail : " + res.error_msg);
				throw new Exception("execute sed to get " + exportKey + " from bash_profile fail : " + res.error_msg);
			}
			oldExportLine = res.sysout.trim();
			
			//1.2 如果存在exportKey，则做修改
			if(oldExportLine != null && !"".equals(oldExportLine.trim())){
				if(oldExportLine.contains("\n")){//包含多行export,取最后一个
					oldExportLine = oldExportLine.substring(oldExportLine.lastIndexOf("\n"));
				}
				oldExportLine = oldExportLine.replaceAll("/", "\\\\/").trim();
				res = execute.execute("sed -i 's/"+oldExportLine+"/" + exportLine + "/g' " + bashFile);
				if(!res.isSuccess){
					log.error("execute sed to update " + exportKey + " in bash_profile fail : " + res.error_msg);
					throw new Exception("execute sed to update " + exportKey + " in bash_profile fail : " + res.error_msg);
				}
			}
			//不存在exportKey，则需要添加
			else{
				//1.3 获取PATH定义
				res = execute.execute("sed -n '/^PATH/p' " + bashFile);
				String pathLine = null;
				if(!res.isSuccess){
					log.error("execute sed to get PATH from bash_profile fail : " + res.error_msg);
					throw new Exception("execute sed to get PATH from bash_profile fail : " + res.error_msg);
				}
				pathLine = res.sysout.trim();
				
				//1.4 如果存在PATH，则把exportKey写到PATH的下一行
				if(pathLine != null && !"".equals(pathLine.trim())){
					pathLine = pathLine.replaceAll("/", "\\\\/");
					res = execute.execute("sed -i '/" + pathLine + "/a\\ \\n" + exportLine + "' " + bashFile);//写入exportLine
					if(!res.isSuccess){
						log.error("execute sed to write " + exportKey + " below path line into bash_profile fail : " + res.error_msg);
						throw new Exception("execute sed to write " + exportKey + " below path line into bash_profile fail : " + res.error_msg);
					}
				}
				//1.5  如果不存在PATH，将JAVA_HOME写入文件最后一行
				else{
					res = execute.execute("sed -i '$a " + " + exportLine + " + "' " + bashFile);//写入exportLine
					if(!res.isSuccess){
						log.error("execute sed to write " + exportKey + " at last line into bash_profile fail : " + res.error_msg);
						throw new Exception("execute sed to write " + exportKey + " at last line into bash_profile fail : " + res.error_msg);
					}
				}
			}
			
			if(addIntoPathValue == null || "".equals(addIntoPathValue)){//不需要向PATH中添加内容
				return;
			}
			//开始在PATH中添加addIntoPathValue,比如：$JAVA_HOME/bin
			//2.1 查找存在的export PATH
			res = execute.execute("sed -n '/^export \\{1,\\}PATH=*/p' " + bashFile);
			String exportPathLine = null;
			if(!res.isSuccess){
				log.error("execute sed to get exist 'export PATH' from bash_profile fail : " + res.error_msg);
				throw new Exception("execute sed to get exist 'export PATH' from bash_profile fail : " + res.error_msg);
			}
			exportPathLine = res.sysout.trim();
			
			//存在export PATH
			if(exportPathLine != null && !"".equals(exportPathLine.trim())){
				//2.2 export PATH中已经存在addIntoPathValue
				if(exportPathLine.contains(addIntoPathValue)){
					log.info(addIntoPathValue + " is exist in export PATH...");
					return;
				}
				//2.2 export PATH中不存在addIntoPathValue，则在已有的export PATH中添加addIntoPathValue
				else{
					if(exportPathLine.contains("\n")){//包含多行export PATH
						exportPathLine = exportPathLine.substring(0, exportPathLine.indexOf("\n"));
					}
					
					String newExportPath;
					if(exportPathLine.contains("=")){
						newExportPath = exportPathLine.replaceFirst("=", "=" + Matcher.quoteReplacement(addIntoPathValue) + ":");
					}else{
						newExportPath = exportPathLine + "=" + Matcher.quoteReplacement(addIntoPathValue) + ":$PATH";
					}
					
					exportPathLine = exportPathLine.replaceAll("/", "\\\\/");
					newExportPath = newExportPath.replaceAll("/", "\\\\/");
					res = execute.execute("sed -i 's/"+exportPathLine+"/" + newExportPath + "/g' " + bashFile);
					if(!res.isSuccess){
						log.error("execute sed to write " + addIntoPathValue + " into 'export PATH' line into bash_profile fail : " + res.error_msg);
						throw new Exception("execute sed to write " + addIntoPathValue + " into 'export PATH' line into bash_profile fail : " + res.error_msg);
					}
				}
			}
			//2.3 不存在export PATH,则添加新的export path，其中包含addIntoPathValue
			else{
				res = execute.execute("sed -i '$a export PATH=" + addIntoPathValue + ":$PATH' " + bashFile);
				if(!res.isSuccess){
					log.error("execute sed to add new 'export PATH' into bash_profile fail : " + res.error_msg);
					throw new Exception("execute sed to add new 'export PATH' into bash_profile fail : " + res.error_msg);
				}
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}
	
	/**
	 * 更新节点中hadoop-env.sh中的JAVA_HOME
	 */
	public static void updateJavaHomeInHadoopEnv(Host host, String fileDir, String javaHome) throws Exception{
		if(javaHome == null || "".equals(javaHome)){
			throw new Exception("no javaHome!");
		}
		
		String hadoopEnvFile = fileDir + "/hadoop-env.sh";
		String javaHomeExport = "export JAVA_HOME=" + javaHome.replaceAll("/", "\\\\/");
		
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			//开始添加JAVA_HOME
			//1. 获取已存在的JAVA_HOME设置
			Result res = execute.execute("sed -n '/^export \\{1,\\}JAVA_HOME=*/p' " + hadoopEnvFile);
			String oldJavaHome = null;
			if(!res.isSuccess){
				log.error("execute sed to get JAVA_HOME from hadoop-env.sh fail : " + res.error_msg);
				throw new Exception("execute sed to get JAVA_HOME from hadoop-env.sh fail : " + res.error_msg);
			}
			oldJavaHome = res.sysout.trim();
			
			//2. 如果存在JAVA_HOME，则做修改
			if(oldJavaHome != null && !"".equals(oldJavaHome.trim())){
				oldJavaHome = oldJavaHome.replaceAll("/", "\\\\/");
				res = execute.execute("sed -i 's/"+oldJavaHome+"/" + javaHomeExport + "/g' " + hadoopEnvFile);
				if(!res.isSuccess){
					log.error("execute sed to update JAVA_HOME in hadoop-env.sh fail : " + res.error_msg);
					throw new Exception("execute sed to update JAVA_HOME in hadoop-env.sh fail : " + res.error_msg);
				}
			}
			//3. 不存在JAVA_HOME，将JAVA_HOME写入文件最后一行
			else{
				res = execute.execute("sed -i '$a " + javaHomeExport + "' " + hadoopEnvFile);//写入JAVA_HOME
				if(!res.isSuccess){
					log.error("execute sed to write JAVA_HOME at last line into hadoop-env.sh fail : " + res.error_msg);
					throw new Exception("execute sed to write JAVA_HOME at last line into hadoop-env.sh fail : " + res.error_msg);
				}
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}
	
	/**
	 * 更新节点中hadoop-env.sh中的HADOOP_SSH_OPTS,比如：export HADOOP_SSH_OPTS="-p 65522"
	 */
	public static void updateSSHOptsInHadoopEnv(Host host, String fileDir, String opt) throws Exception{
		if(opt == null || "".equals(opt)){
			throw new Exception("no opt!");
		}
		
		String hadoopEnvFile = fileDir + "/hadoop-env.sh";
		String sshOptsExport = "export HADOOP_SSH_OPTS=\"" + opt.replaceAll("/", "\\\\/") + "\"";
		
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			//1. 获取已存在的HADOOP_SSH_OPTS设置
			Result res = execute.execute("sed -n '/^export \\{1,\\}HADOOP_SSH_OPTS=*/p' " + hadoopEnvFile);
			String oldSSHOpts = null;
			if(!res.isSuccess){
				log.error("execute sed to get HADOOP_SSH_OPTS from hadoop-env.sh fail : " + res.error_msg);
				throw new Exception("execute sed to get HADOOP_SSH_OPTS from hadoop-env.sh fail : " + res.error_msg);
			}
			oldSSHOpts = res.sysout.trim();
			
			//2. 如果存在HADOOP_SSH_OPTS，则做修改
			if(oldSSHOpts != null && !"".equals(oldSSHOpts.trim())){
				oldSSHOpts = oldSSHOpts.replaceAll("/", "\\\\/");
				res = execute.execute("sed -i 's/" + oldSSHOpts + "/" + sshOptsExport + "/g' " + hadoopEnvFile);
				if(!res.isSuccess){
					log.error("execute sed to update HADOOP_SSH_OPTS in hadoop-env.sh fail : " + res.error_msg);
					throw new Exception("execute sed to update HADOOP_SSH_OPTS in hadoop-env.sh fail : " + res.error_msg);
				}
			}
			//3. 不存在HADOOP_SSH_OPTS，将HADOOP_SSH_OPTS写入文件最后一行
			else{
				res = execute.execute("sed -i '$a " + sshOptsExport + "' " + hadoopEnvFile);//写入HADOOP_SSH_OPTS
				if(!res.isSuccess){
					log.error("execute sed to write HADOOP_SSH_OPTS at last line into hadoop-env.sh fail : " + res.error_msg);
					throw new Exception("execute sed to write HADOOP_SSH_OPTS at last line into hadoop-env.sh fail : " + res.error_msg);
				}
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}
	
	/**
	 * 更新节点中slaves中的节点
	 */
	public static void updateSlaves(Host host, String fileDir, Host[] slaves) throws Exception{
		if(slaves == null || slaves.length == 0){
			throw new Exception("no slaves!");
		}
		
		String slavesFile = fileDir + "/slaves";
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			String[] cmds = new String[slaves.length];
			cmds[0] = "echo " + slaves[0].getHostname() + " > " + slavesFile;
			for(int i=1; i<slaves.length; i++){
				cmds[i] = "echo " + slaves[i].getHostname() + " >> " + slavesFile;
			}
			Result res = execute.execute(cmds);
			if(!res.isSuccess){
				log.error("execute echo to write slave into file slaves fail : " + res.error_msg);
				throw new Exception("execute echo to write slave into file slaves fail : " + res.error_msg);
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}
	
	/**
	 * 更新xml文件
	 */
	public static void updateXml(Host host, String filePath, Map<String, String> nodes) throws Exception{
		if(nodes == null || nodes.size() == 0){
			throw new Exception("no nodes!");
		}
		
		ExecuteLinuxSSHCMD execute = null;
		try{
			Result res = null;
			execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			int num = 0;
			String xmlContent = null;
			while(num++ < 2){//此处偶尔会获取不到内容，故重试一次
				res = execute.execute("cat " + filePath);//获取xml文件内容
				if(!res.isSuccess){
					log.error("execute cat to get xml content fail : " + res.error_msg);
					throw new Exception("execute cat to get xml content fail : " + res.error_msg);
				}
				xmlContent = res.sysout;
				if(xmlContent.indexOf("<configuration>") < 0){//没有获取内容
					System.out.println("============" + num);
					System.out.println(xmlContent);
					System.out.println(xmlContent.indexOf("<configuration>"));
					System.out.println("============");
				}else{//获取到了
					break;
				}
			}
			
			String xmlHeader = xmlContent.substring(0, xmlContent.indexOf("<configuration>"));
			String propertyStr = xmlContent.substring(xmlContent.indexOf("<configuration>") + "<configuration>".length(), xmlContent.indexOf("</configuration>")).trim();
			while(true){//清除注释内容
				if(propertyStr.contains("<!--")){
					int start = propertyStr.indexOf("<!--");
					int end = propertyStr.indexOf("-->") + 3;
					propertyStr = propertyStr.replaceFirst(propertyStr.substring(start, end), "");
					continue;
				}
				break;
			}
			String[] properties = propertyStr.split("<property>");
			
			Map<String, String> all = new LinkedHashMap<String, String>();
			for(String pro : properties){
				if(pro == null || "".equals(pro.trim())){
					continue;
				}
				String key = pro.substring(pro.indexOf("<name>")+6, pro.indexOf("</name>"));
				String value = pro.substring(pro.indexOf("<value>")+7, pro.indexOf("</value>"));
				all.put(key.trim(), value.trim());
			}
			all.putAll(nodes);//添加新配置信息，若存在，则覆盖
			
			StringBuffer newXmlContent = new StringBuffer();//修改后的xml内容
			newXmlContent.append(xmlHeader);
			newXmlContent.append("<configuration>\n");
			Iterator<String> it = all.keySet().iterator();
			while(it.hasNext()){
				String key = it.next();
				String value = nodes.get(key);
				newXmlContent.append("<property>").append("\n");
				newXmlContent.append("  <name>" + key + "</name>").append("\n");
				newXmlContent.append("  <value>" + value + "</value>").append("\n");
				newXmlContent.append("</property>").append("\n");
			}
			newXmlContent.append("</configuration>");
			
			String newXmlContentStr = Matcher.quoteReplacement(newXmlContent.toString());//保证在执行linux命令时，不因特殊符号报错
			newXmlContentStr = newXmlContentStr.toString().replaceAll("\"", "\\\\\"");//保证有引号的字符串不丢失引号
			res = execute.execute("echo \"" + newXmlContentStr + "\" > " + filePath);//新内容覆盖xml
			if(!res.isSuccess){
				log.error("execute echo to overwrite xml content fail : " + res.error_msg);
				throw new Exception("execute echo to overwrite xml content fail : " + res.error_msg);
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}
	
	/**
	 * 更新zookeeper中的配置
	 */
	public static void updateZookeeperConfig(Host host, String fileDir, Map<String, String> configs) throws Exception{
		if(configs == null || configs.size() == 0){
			throw new Exception("no zookeeper config");
		}
		
		String zooCfgFile = fileDir + "/zoo.cfg";
		
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			Iterator<String> it = configs.keySet().iterator();
			while(it.hasNext()){
				String key = it.next();
				String value = configs.get(key);
			
				String newConfig = key + "=" + value.replaceAll("/", "\\\\/");
				
				//开始添加key
				//1. 获取已存在的key设置
				Result res = execute.execute("sed -n '/^" + key + "=*/p' " + zooCfgFile);
				String oldConfig = null;
				if(!res.isSuccess){
					log.error("execute sed to get " + key + " from " + zooCfgFile + " fail : " + res.error_msg);
					throw new Exception("execute sed to get " + key + " from " + zooCfgFile + " fail : " + res.error_msg);
				}
				oldConfig = res.sysout.trim();
				
				//2. 如果存在key，则做替换
				if(oldConfig != null && !"".equals(oldConfig.trim())){
					oldConfig = oldConfig.replaceAll("/", "\\\\/");
					res = execute.execute("sed -i 's/" + oldConfig + "/" + newConfig + "/g' " + zooCfgFile);
					if(!res.isSuccess){
						log.error("execute sed to update " + key + " in " + zooCfgFile + " fail : " + res.error_msg);
						throw new Exception("execute sed to update " + key + " in " + zooCfgFile + " fail : " + res.error_msg);
					}
				}
				//3. 不存在key，将config写入文件最后一行
				else{
					res = execute.execute("sed -i '$a " + newConfig + "' " + zooCfgFile);//写入config
					if(!res.isSuccess){
						log.error("execute sed to write " + newConfig + " at last line into " + zooCfgFile + " fail : " + res.error_msg);
						throw new Exception("execute sed to write " + newConfig + " at last line into " + zooCfgFile + " fail : " + res.error_msg);
					}
				}
			}
		}finally{
			if(execute != null)
				execute.close();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			Host host = new Host("192.168.58.129", null, 22, null, "liulu", "liulu");
			
			Map<String, String> configs = new LinkedHashMap<String, String>();
			configs.put("dataDir", "/home/liulu/data/zookeeper");
			configs.put("clientPort", "9999");
			configs.put("server.1", "192.168.58.129:2888:3888");
			configs.put("server.2", "192.168.58.130:2889:3889");
			updateZookeeperConfig(host, "/home/liulu/test", configs);
			
//			String javaHome = "/home/ocetl/app/java";
//			updateJavaHomeInBashProfile(host, javaHome);
			
//			Map<String, String> mapredSite = new HashMap<String, String>();
//			mapredSite.put("mapred.job.tracker", "localhost:9001");
//			mapredSite.put("mapred.temp.dir", "${hadoop.tmp.dir}/mapred/temp");
//			updateXml(host, "/home/liulu/yarn-site.xml", mapredSite);
						
//			updateJavaHomeInHadoopEnv(host, javaHome);
			
			String opt = "-p 65522";
//			updateSSHOptsInHadoopEnv(host, opt);
			
			String[] slaves = new String[]{
					"aaa",
					"bbb",
					"ccc"
			};
//			updateSlaves(host, "/home/ocetl", slaves);
			
			Map<String, String> site = new LinkedHashMap<String, String>();
			site.put("yarn.nodemanager.aux-services", "mapreduce.shuffle");
			site.put("yarn.nodemanager.aux-services.mapreduce.shuffle.class", "org.apache.hadoop.mapred.ShuffleHandler");
			site.put("yarn.resourcemanager.hostname", "master");
			site.put("yarn.resourcemanager.address", "${yarn.resourcemanager.hostname}:8032");
			site.put("yarn.resourcemanager.scheduler.address", "${yarn.resourcemanager.hostname}:8030");
			site.put("yarn.resourcemanager.webapp.address", "${yarn.resourcemanager.hostname}:8088");
			site.put("yarn.resourcemanager.webapp.https.address", "${yarn.resourcemanager.hostname}:8090");
			site.put("yarn.resourcemanager.resource-tracker.address", "${yarn.resourcemanager.hostname}:8031");
			site.put("yarn.resourcemanager.admin.address", "${yarn.resourcemanager.hostname}:8033");
			
//			updateXml(host, "/home/liulu/yarn-site.xml", site);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

