package autodeploy.common.host;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import autodeploy.pack.Package;

/**
 * liulu5
 * 2014-7-15
 */
public class Host {

	String ip;
	String hostname;
	int sshPort;
	String rootpwd;
	String username;
	String password;
	
	public Host(String ip, String hostname, int sshPort2, String rootpwd, String username, String password) {
		this.ip = ip;
		this.hostname = hostname;
		this.sshPort = sshPort2;
		this.rootpwd = rootpwd;
		this.username = username;
		this.password = password;
	}
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	
	public int getSshPort() {
		return sshPort;
	}

	public void setSshPort(int sshPort) {
		this.sshPort = sshPort;
	}

	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}

	public String getRootpwd() {
		return rootpwd;
	}

	public void setRootpwd(String rootpwd) {
		this.rootpwd = rootpwd;
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof Host)){
			return false;
		}
		Host other = (Host) obj;
		if(this.ip == null || "".equals(this.ip) || other.ip == null || "".equals(other.ip)){
			return false;
		}
		return this.ip.equals(other.ip);
	}
	
	public static void main(String[] args) {
		Host h1 = new Host("192.168.1.2", null, 22, null, null, null);
		Host h2 = new Host("192.168.1.2", null, 22, null, null, null);
		
		System.out.println(h1.hashCode());
		System.out.println(h2.hashCode());
		
		Map<Host, String> relation = new HashMap<Host, String>();
		relation.put(h1, "aaa");
		System.out.println(relation.containsKey(h2));
	}
}

