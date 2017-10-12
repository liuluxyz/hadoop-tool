package autodeploy.createuser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import autodeploy.common.host.Host;

import common.linux.ExecuteLinuxSSHCMD;

import net.neoremind.sshxcute.core.Result;

/**
 * 创建linux普通用户
 * @author liulu5
 * 2014.5.6
 */
public class CreateUser {
	
	private static final Log log = LogFactory.getLog(CreateUser.class);
	
	private Host[] hosts;
	
	public CreateUser(Host[] hosts){
		this.hosts = hosts;
	}
	
	public void startCreate() throws Exception{
		log.info("start create user...");
		
		String filename = null;
		for(Host host : hosts){
			filename = writePWDFile(host.getPassword());
			log.info("start create user on : " + host.getIp());
			ExecuteLinuxSSHCMD execute = new ExecuteLinuxSSHCMD(host.getIp(), host.getSshPort(), "root", host.getRootpwd());
			execute.uploadSingleDataToServer(filename, "/root");
			
			Result res = execute.execute("useradd " + host.getUsername());
			log.info("create user : " + res.isSuccess);
			
			res = execute.execute("passwd " + host.getUsername() + " < " + filename);
			log.info("passwd user : " + res.isSuccess);
			
			res = execute.execute("rm /root/" + filename);
			log.info("rm tmp file : " + res.isSuccess);
			
			execute.close();
			log.info("end create user on : " + host.getIp());
		}
		
		removeFile(filename);//remove temp file
		log.info("end create user...");
	}
	
	public void setHosts(Host[] hosts) {
		this.hosts = hosts;
	}

	private String writePWDFile(String password){
		String filename = "pwdFile";
		File file = new File(filename);
		if(file.exists()){
			file.delete();
		}
		
		try {
			FileOutputStream stream = new FileOutputStream(file);
			stream.write((password + "\n" + password).getBytes());
			stream.flush();
			stream.close();
		} catch (FileNotFoundException e) {
			log.error("writePWDFile error : ", e);
		} catch (IOException e) {
			log.error("writePWDFile error : ", e);
		}
		return filename;
	}
	
	private void removeFile(String filename){
		if(filename == null || "".equals(filename)){
			return;
		}
		File file = new File(filename);
		if(file.exists()){
			file.delete();
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
//		if(args.length == 0){
//			log.info("input IP, root username, root password, new username, new password...");
//			return;
//		}
//		CreateUser ssh = new CreateUser(args);
//		ssh.startCreate();
		
//		String[] ips = new String[]{"10.1.22.94", "10.1.22.16", "10.1.22.20"};
//		String[] ips = new String[]{"10.1.253.177","10.1.253.181","10.1.253.185"};
//		String rootpassword = "asiainfo";
//		String newusername = "liulu2";
//		String newpassword = "liulu2";
		
		Host[] hosts = new Host[]{
				new Host("10.1.253.26", null, 22, "asiainfo", "liulu", "liulu"),
				new Host("10.1.253.27", null, 22, "asiainfo", "liulu", "liulu"),
				new Host("10.1.253.28", null, 22, "asiainfo", "liulu", "liulu"),
				new Host("10.1.253.29", null, 22, "asiainfo", "liulu", "liulu"),
		};
		
		CreateUser ssh = new CreateUser(hosts);
		try {
			ssh.startCreate();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
