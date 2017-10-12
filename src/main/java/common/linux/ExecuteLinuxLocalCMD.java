package common.linux;

import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * liulu5 2014-2-21
 */
public class ExecuteLinuxLocalCMD {
	
	private static final Log log = LogFactory.getLog(ExecuteLinuxLocalCMD.class);
	
	public static String exec(String cmd) {
		try {
			String[] cmdA = { "/bin/sh", "-c", cmd };
			Process process = Runtime.getRuntime().exec(cmdA);
			LineNumberReader br = new LineNumberReader(new InputStreamReader(process.getInputStream()));
			StringBuffer sb = new StringBuffer();
			String line;
			while ((line = br.readLine()) != null) {
				log.info(line);
				sb.append(line).append("\n");
			}
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void main(String[] args) {

		String pwdString = exec("pwd").toString();
//		String netsString = exec("netstat -nat|grep -i \"80\"|wc -l").toString();
		System.out.println("==========获得值=============");
		System.out.println(pwdString);
	}
}
