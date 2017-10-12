package common.linux;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import autodeploy.common.host.Host;

/**
 * liulu5
 * 2015-5-28
 */
/**
 * @Description: TODO
 * @author liulu5
 * @date 2015-5-28 上午1:02:16 
 */
public class Test {
	
	
	private static void test(int num) throws Exception{
		
		
		

//3. scp "data" host:/data/datac/test

//4. hdfs dfs -put /data/datac/test/d



		
		for(int i=11; i<23; i++){
//			mkdir dir /data/datac/test
			
			
				String[] cmdstrs = new String[]{
						"ll /data/datac/*"
				};

				String cmd = "scp /root/f 192.168.95." + i + ":/root/";
//				Host h = new Host("192.168.95.10", null, 22, "11111111", "root", "11111111");
				final Host h = new Host("192.168.96." + i, null, 22, "11111111", "root", "11111111");
				
//				ExecuteLinuxSSHCMD execute = new ExecuteLinuxSSHCMD(h.getIp(), h.getSshPort(), h.getUsername(), h.getPassword());
//				execute.uploadSingleDataToServer("C:\\Users\\liulu5\\Desktop\\openssh-clients-5.3p1-84.1.el6.x86_64.rpm", "/home");
//				execute.uploadSingleDataToServer("C:\\Users\\liulu5\\Desktop\\libedit-2.11-4.20080712cvs.1.el6.x86_64.rpm", "/home");
//				execute.close();

				LinuxHelper.execute(h, cmdstrs);
				
			
		}
	}
	
	private static void prepare(int num) throws Exception{
		Host sourceHost = new Host("192.168.96.22", null, 22, "11111111", "root", "11111111");
		
		for(int i=0; i<num; i++){
			for(int j=11; j<23; j++){
				String dir = "/tmp/test/" + j + "/" + i;
				LinuxHelper.execute(sourceHost, "hdfs dfs -mkdir -p " + dir);
			}	
		}
		
		for(int j=11; j<23; j++){
			Host host = new Host("192.168.96." + j, null, 22, "11111111", "root", "11111111");
			for(int i=0; i<num; i++){
				switch(i){
				case 0:
					LinuxHelper.execute(host, "mkdir  /data/datac/test");
					break;
				case 1:
					LinuxHelper.execute(host, "mkdir  /data/datad/test");
					break;
				case 2:
					LinuxHelper.execute(host, "mkdir  /data/datae/test");
					break;
				case 3:
					LinuxHelper.execute(host, "mkdir  /data/dataf/test");
					break;
				default:
				}
			}
		}
	}

	
	private static void distribute(){
		List<Host> hosts = new ArrayList<Host>();
		int num = 0;
		for(int i=12; i<17; i++){
			Host host = new Host("134.160.37." + i, "ochadoop" + i, 22, "asiainfo", "ocnosql", "ocnosql");
			hosts.add(host);
		}
		
		Host source = new Host("134.160.37.11", "ochadoop11", 22, "asiainfo", "ocnosql", "ocnosql");
		
		for(Host host : hosts){
			String cmd = "scp -r /home/ocnosql/app/jdk1.7.0_67 " + host.getHostname() + ":/home/ocnosql/app/";
			try {
				LinuxHelper.execute(source, cmd);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("eeeeee");
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
//			prepare(4);
			
			distribute();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}

