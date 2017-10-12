package autodeploy.pack;

import autodeploy.common.ServiceEnum;

/**
 * liulu5
 * 2014-5-28
 */
public class Package {

	private ServiceEnum service;
	private String name;//安装包的文件名
	private String path;//安装包的文件路径
	private String rootDir;//安装包中的根目录名
	private String deployDir;//部署后的目录命名
	private String installCMD;//安装命令
	
	public Package(){
		
	}
	public Package(String path){
		this.path = path;
	}
	
	public ServiceEnum getService() {
		return service;
	}
	public void setService(ServiceEnum service) {
		this.service = service;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getInstallCMD() {
		return installCMD;
	}
	public void setInstallCMD(String installCMD) {
		this.installCMD = installCMD;
	}
	public String getRootDir() {
		return rootDir;
	}
	public void setRootDir(String rootDir) {
		this.rootDir = rootDir;
	}
	public String getDeployDir() {
		return deployDir;
	}
	public void setDeployDir(String deployDir) {
		this.deployDir = deployDir;
	}
}

