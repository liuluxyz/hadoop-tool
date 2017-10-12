package common.linux;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import autodeploy.common.host.Host;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.IOptionName;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;
import net.neoremind.sshxcute.task.impl.ExecShellScript;

public class ExecuteLinuxSSHCMD {

	private static final Log log = LogFactory.getLog(ExecuteLinuxSSHCMD.class);
	SSHExec ssh;
	
//	public ExecuteLinuxSSHCMD(String ipAddr, String username, String password) throws Exception{
//		ConnBean cb = new ConnBean(ipAddr, username, password);
//		ssh = new SSHExec(cb);
//		boolean res = ssh.connect();
//		if(res == false){
//			throw new Exception("SSH connect fails with the following info : " + ipAddr + "," + username + "," + password);
//		}
//	}
	
	public ExecuteLinuxSSHCMD(Host host) throws Exception{
		SSHExec.setOption(IOptionName.SSH_PORT_NUMBER, host.getSshPort());
		ConnBean cb = new ConnBean(host.getIp(), host.getUsername(), host.getPassword());
		ssh = new SSHExec(cb);
		boolean res = ssh.connect();
		if(res == false){
			throw new Exception("SSH connect fails with the following info : " + host.getIp() + "," + host.getUsername() + "," + host.getPassword());
		}
	}
	
	public ExecuteLinuxSSHCMD(String ipAddr, int port, String username, String password) throws Exception{
		SSHExec.setOption(IOptionName.SSH_PORT_NUMBER, port);
		ConnBean cb = new ConnBean(ipAddr, username, password);
		ssh = new SSHExec(cb);
		boolean res = ssh.connect();
		if(res == false){
			throw new Exception("SSH connect fails with the following info : " + ipAddr + "," + username + "," + password);
		}
	}
	
//	public Result execute(String cmd){
//		Result res = null;
//		try {
//			CustomTask ct1 = new ExecCommand(cmd);
//			res = ssh.exec(ct1);
//			if (res.isSuccess){
//				log.info("Return code: " + res.rc);
//				log.info("sysout: " + res.sysout);
//			}
//			else{
//				log.info("Return code: " + res.rc);
//				log.info("error message: " + res.error_msg);
//			}
//		}catch (TaskExecFailException e){
//			log.error(e.getMessage(), e);
//		}
//		catch (Exception e){
//			log.error(e.getMessage(), e);
//		}
//		return res;
//	}
	
	public Result execute(String... cmds){
		Result res = null;
		try {
			CustomTask ct1 = new ExecCommand(cmds);
			res = ssh.exec(ct1);
			if (res.isSuccess){
				log.info("Return code: " + res.rc);
				log.info("sysout: " + res.sysout);
			}
			else{
				log.warn("Return code: " + res.rc);
				log.warn("error message: " + res.error_msg);
			}
		}
		catch (Exception e){
			log.error(e.getMessage(), e);
		}
		return res;
	}
	
	public void executeIgnoreError(String... cmds){
		try {
			CustomTask ct1 = new ExecCommand(cmds);
			Result res = ssh.exec(ct1);
			if (res.isSuccess){
				log.info("Return code: " + res.rc);
				log.info("sysout: " + res.sysout);
			}
			else{
				log.warn("Return code: " + res.rc);
				log.warn("error message: " + res.error_msg);
			}
		}
		catch (Exception e){
			log.warn(e.getMessage(), e);
		}
	}
	
	public void executeShellScript(String workingDir, String shellPath, String args){
		try {
			CustomTask ct2 = new ExecShellScript(workingDir, shellPath, args);
			Result res = ssh.exec(ct2);
			if (res.isSuccess)
			{
			log.info("Return code: " + res.rc);
			log.info("sysout: " + res.sysout);
			}
			else
			{
			log.info("Return code: " + res.rc);
			log.info("error message: " + res.error_msg);
			}
		}
		catch (TaskExecFailException e){
			log.error(e.getMessage(), e);
		}
		catch (Exception e){
			log.error(e.getMessage(), e);
		}
	}
	
	public void uploadSingleDataToServer(String localFile, String toServer){
		try {
			ssh.uploadSingleDataToServer(localFile, toServer);
		}
		catch (TaskExecFailException e){
			log.error(e.getMessage(), e);
		}
		catch (Exception e){
			log.error(e.getMessage(), e);
		}
	}
	
	public void close(){
		ssh.disconnect();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Host name1 = new Host("10.4.56.6", null, 22, null, "ocdp", "ocdp@,123");
//		String cmd = "nohup /home/liulu/app/apache-hive-1.2.1-bin/bin/hive --service metastore > /home/liulu/app/apache-hive-1.2.1-bin/bin/metastore.log 2>&1 &";
		String cmd = "hostname";
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(name1);
			Result res = execute.execute(cmd);
			if(!res.isSuccess){
				System.out.println(res.error_msg);
				System.out.println(res.sysout);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			execute.close();
		}
		
		
//		Host name1 = new Host("192.168.58.133", null, 22, null, "liulu", "liulu");
//		Host name2 = new Host("192.168.58.130", null, 22, null, "liulu", "liulu");
//		
//		Map<String[], Host> cmds = new LinkedHashMap<String[], Host>();
//		
//		cmds.put(new String[]{"source /home/liulu/.bash_profile", 
//				"/home/liulu/app/hadoop/sbin/hadoop-daemons.sh start journalnode",
//				"/home/liulu/app/hadoop/bin/hdfs zkfc -formatZK -force",
//				"/home/liulu/app/hadoop/bin/hdfs namenode -format mycluster -force"}, name1);
//		
//		cmds.put(new String[]{"source /home/liulu/.bash_profile", 
//				"/home/liulu/app/hadoop/bin/hdfs namenode -bootstrapStandby -force",
//				"/home/liulu/app/hadoop/sbin/start-dfs.sh"}, name2);
//		
////		String[] cmd = new String[]{"source /home/liulu/.bash_profile", "/home/liulu/app/zookeeper/bin/zkServer.sh start"};
//		Iterator<String[]> it = cmds.keySet().iterator();
//		while(it.hasNext()){
//			String[] cmd = it.next();
//			Host host = cmds.get(cmd);
//			ExecuteLinuxSSHCMD execute = null;
//			try{
//				execute = new ExecuteLinuxSSHCMD(host);
//				Result res = execute.execute(cmd);
//				if(!res.isSuccess){
//					log.error("fail : " + res.error_msg);
//				}
//				log.info(res.sysout);
//			}catch(Exception e){
//				log.error("fail", e);
//			}
//			finally{
//				execute.close();
//			}
//		}
	}

}
