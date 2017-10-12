package autodeploy.createSSH;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import autodeploy.common.host.Host;

import common.linux.ExecuteLinuxSSHCMD;
import common.linux.LinuxHelper;

import net.neoremind.sshxcute.core.Result;

/**
 * 构造节点间的互信
 * @author liulu5
 * 2013.9.18
 */
public class CreateSSHTrust {

	private static final Log log = LogFactory.getLog(CreateSSHTrust.class);
	
	private final String keyFile = "./authorized_keys";
	private Host[] hosts;
	
	public void set(Host[] hosts){
		this.hosts = hosts;
	}
	
	public boolean init(String[] args){
		if(args.length == 0){
			log.info("请输入参数");
			this.printUsage();
			return false;
		}
		String ip = null;
		String username = null;
		String password = null;
		int port = 22;
		for(int i=0; i<args.length; i++){
			if("-h".equals(args[i])){
				this.printUsage();
				return false;
			}
			if("-ip".equals(args[i])){
				ip = args[i+1];
				i++;
			}
			else if("-u".equals(args[i])){
				username = args[i+1];
				i++;
			}
			else if("-p".equals(args[i])){
				password = args[i+1];
				i++;
			}
			else if("-port".equals(args[i])){
				port = Integer.parseInt(args[i+1]);
				i++;
			}
			else{
				log.info("参数不支持 : " + args[i]);
				this.printUsage();
				return false;
			}
		}
		
		if(ip == null){
			log.info("请输入IP");
			this.printUsage();
			return false;
		}
		if(username == null){
			log.info("请输入用户名");
			this.printUsage();
			return false;
		}
		if(password == null){
			log.info("请输入密码");
			this.printUsage();
			return false;
		}
		
		String[] ips = ip.split(",");
		hosts = new Host[ips.length];
		for(int i=0; i<ips.length; i++){
			hosts[i] = new Host(ips[i], null, port , null, username, password);
		}
		log.info("ip : " + ip);
		log.info("username : " + username);
		log.info("password : " + password);
		return true;
	}
	
	public void printUsage(){
		log.info("用法: xxx.sh [-选项] [参数]");
		log.info("说明: 创建节点的双向互信");
		log.info("选项包括:");
		log.info("	-h" 			+ "	显示帮助信息");
		log.info("	-ip"			+ "	创建互信的节点ip，以逗号分隔");
		log.info("	-u"				+ "	节点用户名");
		log.info("	-p"				+ "	节点密码");
		log.info("	-port"			+ "	ssh端口，默认为22");
	}
	
	public void initHostname() throws Exception{
		log.info("start init hostname...");
		for(Host host : hosts){
			String hostname = LinuxHelper.parseHostname(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			host.setHostname(hostname);
		}
		log.info("end init hostname...");
	}
	
	public void startCreate() throws Exception{
		log.info("start create ssh trust...");
		String[] keys = new String[hosts.length];
		int num = 0;
		for(Host host : hosts){//generate key
			log.info("start generate ssh key : " + host.getIp());
			ExecuteLinuxSSHCMD execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			execute.execute("rm ~/.ssh/*");
			execute.execute("ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa");
			execute.execute("chmod 600 ~/.ssh/*");
//			excute.execute("ssh-add ~/.ssh/id_dsa");
			
			Result getKey = execute.execute("cat ~/.ssh/id_dsa.pub");
			keys[num++] = getKey.sysout;
			execute.close();
			log.info("end generate ssh key : " + host.getIp());
		}
		
		writeFile(keys);//write all node's key into temp file : authorized_keys
		
		for(Host host : hosts){//upload authorized_keys to every node
			log.info("start upload key file to host : " + host.getIp());
			ExecuteLinuxSSHCMD execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
			execute.uploadSingleDataToServer(keyFile, "~/.ssh");
			execute.close();
			log.info("end upload key file to host : " + host.getIp());
		}
		
		removeFile();//remove temp file
		
		runFirstSSH();//make every node know echo other
		log.info("end create ssh trust...");
	}
	private void writeFile(String[] keys){
		log.info("start write authorized_keys file...");
		File file = new File(keyFile);
		if(file.exists()){
			file.delete();
		}
		
		try {
			FileOutputStream stream = new FileOutputStream(file);
			for(String key : keys){
				stream.write(key.getBytes());	
			}
			stream.flush();
			stream.close();
		} catch (FileNotFoundException e) {
			log.error("", e);
		} catch (IOException e) {
			log.error("", e);
		}
		log.info("end write authorized_keys file...");
	}
	
	private void runFirstSSH() throws Exception{
		log.info("start run first ssh...");
		String[] target = new String[hosts.length * 2 + 3];//ip and hostname
		int index = 0;
		for(Host host : hosts){
			target[index++] = host.getIp();
			target[index++] = host.getHostname();
		}
		target[index++] = "localhost";
		target[index++] = "0.0.0.0";
		target[index] = "127.0.0.1";
		
		for(final Host host : hosts){
			for(final String sshTo : target){
				final ExecuteLinuxSSHCMD execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword());
				new Thread(){
					public void run(){
						execute.executeIgnoreError("ssh -p " + host.getSshPort() + " " + sshTo + " -oStrictHostKeyChecking=no");//此处有bug：使用的port是源主机的port，非目标主机的port，暂时不修复
					}
				}.start();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				execute.close();
			}
		}
		log.info("end run first ssh...");
	}
	
	private void removeFile(){
		log.info("start remove file...");
		File file = new File(keyFile);
		if(file.exists()){
			file.delete();
		}
		log.info("end remove file...");
	}
	
//	public void setIps(String[] ips) {
//		this.ips = ips;
//	}
//	public void setUsername(String username) {
//		this.username = username;
//	}
//	public void setPassword(String password) {
//		this.password = password;
//	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
//		try {
//			CreateSSHTrust ssh = new CreateSSHTrust();
//			boolean initRes = ssh.init(args);
//			if(initRes == false){
//				return;
//			}
//			ssh.initHostname();
//			ssh.startCreate();
//		} catch (Exception e) {
//			log.error("", e);
//		}
		
//		String[] ips = new String[]{"10.1.22.94", "10.1.22.16", "10.1.22.20"};
//		String[] ips = new String[]{"172.16.2.103","172.16.2.104","172.16.2.106"};
//		String username = "ocetl";
//		String password = "ocetl";
		
		Host[] hosts = new Host[]{
				new Host("10.1.253.26", "ochadoop26", 22, "asiainfo", "liulu", "liulu"),
				new Host("10.1.253.27", "ochadoop27", 22, "asiainfo", "liulu", "liulu"),
				new Host("10.1.253.28", "ochadoop28", 22, "asiainfo", "liulu", "liulu"),
				new Host("10.1.253.29", "ochadoop29", 22, "asiainfo", "liulu", "liulu"),
		};
		
		/**
		Host[] hosts = new Host[24];
		int num = 0;
		for(int i=1; i<23; i++){
			hosts[num++] = new Host("10.4.56." + i, "YSHD" + i, 22, "", "ocdp", "ocdp@,123");
		}
		hosts[num++] = new Host("10.4.56.32", "YSHD24", 22, "", "ocdp", "ocdp@,123");
		hosts[num++] = new Host("10.4.56.33", "YSHD25", 22, "", "ocdp", "ocdp@,123");
		*/
		
		CreateSSHTrust ssh = new CreateSSHTrust();
		ssh.set(hosts);
		try {
			ssh.startCreate();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
