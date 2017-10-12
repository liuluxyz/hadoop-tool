package sd.check;

import hivetool.HiveConnHelper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import common.ReadFileHelper;
import common.ResultData;
import common.db.MYSQLConnHelper;
import common.linux.ExecuteLinuxLocalCMD;

/**
 * 执行多个tcl，然后稽核
 * @author liulu5
 */
public class ExecAndCheck {

	private static final Log log = LogFactory.getLog(ExecAndCheck.class);
	
	String hiveDB = "asiainfo";
	String tclName;
	String[] tcls;
	String optime;
	String[] checkSQLs;
	
	HashMap<String, Integer> tclDuration = new HashMap<String, Integer>();//每个tcl执行的时长,单位为s
	Map<String, Map<String, List<String[]>>> tclCheckData = new HashMap<String, Map<String, List<String[]>>>();//每个tcl对应的稽核结果数据集
	
//	private ResultOutputHelper outputHelper;
	
	public boolean init(String[] args){
		boolean result = initOption(args);
		if(result == false){
			return false;
		}
//		initOutputHelper();
		return true;
	}
	
//	public void initOutputHelper(){
//		if(option.getOutputPath() != null){
//			outputHelper = new ResultOutputHelper(option.getOutputPath());
//		}
//		else{
//			outputHelper = new ResultOutputHelper();
//		}
//	}
	
	public boolean initOption(String[] args){
		int num = 0;
		while(num < args.length){
			try{
				if("-h".equals(args[num])){
					this.printUsage();
					return false;
				}
				else if("-tcl".equals(args[num])){
					tcls = args[num+1].split(";");
					num += 2;
					if(tcls != null && tcls.length > 0){
						this.tclName = tcls[0].substring(tcls[0].lastIndexOf("/")+1);
					}
					continue;
				}
				else if("-optime".equals(args[num])){
					optime = args[num + 1];
					num += 2;
					continue;
				}
				else if("-checksql".equals(args[num])){
					checkSQLs = args[num+1].split(";");
					num += 2;
					continue;
				}
				else if("-checkfile".equals(args[num])){
					boolean res = this.initcheckSQLsFromFile(args[num+1]);
					if(res == false){
						return false;
					}
					num += 2;
					continue;
				}
				else{
					log.info("error: not support parameter : " + args[num]);
					this.printUsage();
					return false;
				}
			}catch(Exception e){
				log.error("error: ", e);
				this.printUsage();
				return false;
			}
		}
		if(tcls == null || tcls.length == 0){
			log.warn("请指定tcl");
			this.printUsage();
			return false;
		}
		if(this.tclName == null || tclName.length() == 0){
			log.warn("无法从tcl路径中解析出名称");
			this.printUsage();
			return false;
		}
		if(optime == null || "".equals(optime)){
			log.warn("请指定帐期");
			this.printUsage();
			return false;
		}
		
		return true;
	}
	
	public void printUsage(){
		log.info("用法: hadoop jar xxx.jar [-选项] [参数]");
		log.info("	e.g. hadoop jar tool.jar -hql 'select xxx from ...'");
		log.info("选项包括:");
		log.info("	-h	" 				+ "	显示帮助信息");
		log.info("	-tcl	"			+ "	指定tcl路径");
		log.info("	-optime	"			+ "	指定帐期");
		log.info("	-checksql" 			+ "	指定稽核sql");
		log.info("	-checkfile" 		+ "	指定稽核sql所在的文件");
	}
	
	public void start(){
		log.info("start...");
		try {
			for(int i=0; i<tcls.length; i++){
				log.info("progress : 开始第" + (i+1) + "总" + tcls.length);
				//1. 删除mysql中对应的tcl执行记录
				if(deleteExecRecord() == false){
					log.warn("删除已执行记录失败");
					return;
				}
				
				//2. 执行tcl
				execTCL(tcls[i]);
				
				//3. 监控tcl执行结果：执行时间、时长
				monitorTCL(tcls[i]);
				
				//4. 执行稽核语句，记录稽核结果
				getCheckData(tcls[i]);
				
				log.info("progress : 结束第" + (i+1) + "总" + tcls.length);
			}
			
			// 5. 对多个tcl的执行结果进行对比：执行时间对比，执行结果是否相同
			doCheck();
			
			//6. 输出报告
			
		} catch (Exception e) {
			e.printStackTrace();
			log.error("exception : " + e.toString(), e);
		}
		log.info("end...");
	}
	
	private boolean deleteExecRecord() throws ParseException{
		log.info("start deleteExecRecord...");
		String time = new SimpleDateFormat("yyyy-mm-dd").format(new SimpleDateFormat("yyyymmdd").parse(optime));
		String sql = "delete from exec_center where program_name in ('" + this.tclName + "') and op_time='" + time + "'";
		log.info(sql);
		
		int res =  MYSQLConnHelper.getInstance().executeUpdateSQL(sql);
		log.info("deleteExecRecord res : " + res);
		log.info("end deleteExecRecord...");
		
		if(res < 0){
			return false;
		}
		return true;
	}
	
	private void execTCL(String tclPath){
		log.info("start execTCL...");
		String cmd = "ts_ss -s " + tclPath + " -d " + optime + " -u 2289795 -v 160";
		
		log.info("execTCL : " + cmd);
		String msg = ExecuteLinuxLocalCMD.exec(cmd);
		log.info("exec tcl : " + msg);
		log.info("end execTCL...");
	}
	
	private boolean monitorTCL(String tclPath) throws ParseException{
		log.info("enter monitorTCL...");
		
		String time = new SimpleDateFormat("yyyy-mm-dd").format(new SimpleDateFormat("yyyymmdd").parse(optime));
		String sql = "select exec_duration from exec_center where program_name in ('" + this.tclName + "') and op_time='" + time + "'";
		
		long start = System.currentTimeMillis();
		while(true){
			String execDuration = MYSQLConnHelper.getInstance().getOneValue(sql);
			if(execDuration != null && !"".equals(execDuration)){//tcl执行完成
				log.info("tcl执行完成 : [" + execDuration + "] " + tclPath);
				String[] temp = execDuration.split(":");
				int duration = (Integer.parseInt(temp[0]) * 60 * 60) + (Integer.parseInt(temp[1]) * 60) + Integer.parseInt(temp[2]);
				tclDuration.put(tclPath, duration);
				return true;
			}
			long current = System.currentTimeMillis();
			if((current - start) / (1000 * 60 * 60) > 3){
				log.error("tcl执行超过3小时，退出监控 : " + tclPath);
				return false;
			}
			
			try {
				log.info("monitor sleep 180s");
				Thread.sleep(180 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			log.info("doing monitorTCL...");
		}
	}
	
	private void getCheckData(String tclPath) throws Exception{
		log.info("start getCheckData...");
		Map<String, List<String[]>> checkData = new HashMap<String, List<String[]>>();
		for(String sql : checkSQLs){
			log.info("start exec hql : " + sql);
			List<String[]> data = HiveConnHelper.getInstance().executeMultiQuery(hiveDB, sql);
			checkData.put(sql, data);
			log.info("end exec hql : " + sql);
		}
		tclCheckData.put(tclPath, checkData);
		log.info("end getCheckData...");
	}
	
	private boolean doCheck(){
		log.info("start doCheck...");
		
		StringBuffer content = new StringBuffer();
		content.append("**********************************************************").append("\n");
		content.append("tcl数量：" + tcls.length).append("\n");
		content.append("执行时长(s):").append("\n");
		log.info("统计执行时长(s)：");
		for(String tcl : tcls){
			String temp = tcl + " : " + tclDuration.get(tcl);
			content.append(temp).append("\n");
			log.info(temp);
		}
		content.append("**********************************************************").append("\n");
		content.append("tcl稽核sql数量：" + checkSQLs.length).append("\n");
		for(int i=0; i<tcls.length-1; i++){
			for(int j=1; j<tcls.length; j++){
				for(int sqlIndex=0; sqlIndex<checkSQLs.length; sqlIndex++){
					content.append("==================================").append("\n");
					String mes = "对比 : [" + tcls[i] + "] [" + tcls[j] + "] [" + checkSQLs[sqlIndex] + "]";
					content.append(mes).append("\n");
					log.info("开始" + mes);
					ResultData compareRes = compareData(tclCheckData.get(tcls[i]).get(checkSQLs[sqlIndex]), tclCheckData.get(tcls[j]).get(checkSQLs[sqlIndex]));
					if(compareRes.isResult() == false){
						log.info(compareRes.getMessage());
						content.append(compareRes.getMessage()).append("\n");
					}
					content.append("==================================").append("\n");
				}
			}
		}
		
		log.info(content);
		log.info("end doCheck...");
		return true;
	}
	
	private ResultData compareData(List<String[]> data1, List<String[]> data2){
		log.info("start compareData...");
		if(data1 == null || data2 == null || data1.size() == 0 || data2.size() == 0){
			log.info("数据为空");
			return new ResultData(false, "数据为空");
		}
		if(data1.size() != data2.size()){
			log.info("数据条数不同");
			return new ResultData(false, "数据条数不同");
		}
		
		for(int i=0; i<data1.size(); i++){
			String[] value1 = data1.get(i);
			String[] value2 = data2.get(i);
			
			if(value1 == null || value2 == null || value1.length == 0 || value2.length == 0){
				log.info("数据为空");
				return new ResultData(false, "数据为空");
			}
			if(value1.length != value2.length){
				log.info("数据个数不同");
				return new ResultData(false, "数据个数不同");
			}
			for(int index=0; index<value1.length; index++){
				if((value1[index] == null || "".equals(value1[index])) && (value2[index] == null || "".equals(value2[index]))){
					continue;
				}
				else if(value1[index].equals(value2[index])){
					continue;
				}
				else{
					log.info("数据不同");
					return new ResultData(false, "数据不同 : " + Arrays.toString(value1) + Arrays.toString(value2));
				}
			}
		}
		return new ResultData(true, "");
	}
	
	private boolean initcheckSQLsFromFile(String filePath){
		log.info("start initcheckSQLsFromFile...");
		checkSQLs = ReadFileHelper.readLocalFileByLine(filePath);
		if(checkSQLs == null || checkSQLs.length == 0){
			log.info("checkSQLs is empty");
			return false;
		}
		log.info("end initcheckSQLsFromFile...");
		return true;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		ExecAndCheck check = new ExecAndCheck();
		boolean result = check.init(args);
		if(result == false){
			return;
		}
		check.start();
	}
}