package linux;

import net.neoremind.sshxcute.core.Result;

import common.linux.ExecuteLinuxSSHCMD;

/**
 * liulu5
 * 2014-5-30
 */
public class Distribute {

	private static String username = "liulu";
	private static String password = "liulu";
	
	/**
	 * 使用scp命令分发文件
	 * @param filePath
	 * @param targetPath
	 * @param sourceNode
	 * @param targetNodes
	 */
	public static void start(String filePath, String targetPath, String sourceNode, String[] targetNodes){
		ExecuteLinuxSSHCMD execute = null;
		try{
			execute = new ExecuteLinuxSSHCMD(sourceNode, 22, username, password);
			boolean isDir = false;
			Result res = execute.execute("cd " + filePath);
			if(res.isSuccess){
				isDir = true;
			}
			for(String targetNode : targetNodes){
				String cmd;
				if(isDir == true){
					cmd = "scp -r " + filePath + " " + targetNode + ":" + targetPath;	
				}else{
					cmd = "scp " + filePath + " " + targetNode + ":" + targetPath;
				}
				System.out.println("cmd : " + cmd);
				res = execute.execute(cmd);
				if(res.isSuccess){
					System.out.println("success : " + res.sysout);
				}else{
					System.out.println("fail : " + res.error_msg);
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		finally{
			if(execute != null)
				execute.close();
		}	
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String filePath = "/home/liulu/.bash_profile";
		String targetPath = "/home/liulu/";
		String sourceNode = "10.1.253.178";
		String[] targetNodes = new String[]{"10.1.253.179","10.1.253.182","10.1.253.183", "10.1.253.186", "10.1.253.187"};
		start(filePath, targetPath, sourceNode, targetNodes);
		
	}

}

