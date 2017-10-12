package common.linux;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import autodeploy.common.host.Host;
import net.neoremind.sshxcute.core.Result;

/**
 * liulu5
 * 2014-7-2
 */
public class LinuxHelper {

	public static String parseHostname(String ip, int port, String username, String password) throws Exception{
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(ip, port, username, password);
			Result getHostname = execute.execute("hostname");
			if(!getHostname.isSuccess){
				throw new Exception("failed to parse host name : " + getHostname.error_msg);
			}
			return getHostname.sysout.trim();
		}finally{
			if(execute != null)
				execute.close();
		}
	}
	
	public static String execute(String ip, int port, String username, String password, String... cmd) throws Exception{
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(ip, port, username, password);
			Result res = execute.execute(cmd);
			if(!res.isSuccess){
				throw new Exception("execute ssh cmd fail : " + res.error_msg);
			}
			System.out.println("ip : " + ip + "\n" + res.sysout);
			return res.sysout;
		}finally{
			if(execute != null)
				execute.close();
		}
	}
	
	public static String execute(Host host, String... cmds) throws Exception{
		return execute(host.getIp(), host.getSshPort(), host.getUsername(), host.getPassword(), cmds);
	}
	
	public static void main(String[] args) {
		
//		Host host = new Host("10.1.253.179", null, 22, null, "liulu", "liulu");

//		List<Host> hosts = new ArrayList<Host>();
//		int num = 0;
//		for(int i=11; i<17; i++){
//			Host host = new Host("134.160.37." + i, "ochadoop" + i, 22, "asiainfo", "root", "asiainfo");
//			hosts.add(host);
//		}
		
		Host[] hosts = new Host[23];
		int num = 0;
		for(int i=2; i<23; i++){
			hosts[num++] = new Host("10.4.56." + i, "YSHD" + i, 22, "", "ocdp", "ocdp@,123");
		}
		hosts[num++] = new Host("10.4.56.32", "YSHD24", 22, "", "ocdp", "ocdp@,123");
		hosts[num++] = new Host("10.4.56.33", "YSHD25", 22, "", "ocdp", "ocdp@,123");
		
//		String cmd = "rm -fr /data1/hmc/hadoop/hdfs/data/*";
		String cmd = "rm -fr /data1/hmc/hadoop/zookeeper/*";
		
		
		for(Host host : hosts){
			try {
				execute(host, cmd);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("eeeeee");
			}
		}
	}
}

