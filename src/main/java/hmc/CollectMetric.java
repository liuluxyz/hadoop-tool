package hmc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CollectMetric {

	private static Log logger = LogFactory.getLog(CollectMetric.class);
	
    private String collectMetricPath;
	
	private int fileNum = 10;//保存监控指标数据文件的个数
	private int maxFileSize = 50 * 1024;//50M 保存监控指标数据文件的最大大小
	private int currentFileNo = 0;//当前正在写入的文件标识
	
	//每天凌晨00:00:00触发
//	@Scheduled(cron = "0 0 0 * * ?")
	private void collect(){
		logger.info("start collect...");
		/**
		try {
			JSONArray collect = new JSONArray();
			List<ClusterDTO> list = clusterService.findAllClusters();
			for(ClusterDTO cluster : list){
				JSONObject metric = new JSONObject();
				metric.put("cluster", JSONObject.fromObject(cluster));
				
				JSONArray hostMetric = collectHostMetric(cluster.getClusterId());//收集主机监控信息
				metric.put("hostMetric", hostMetric);
				
				JSONArray serviceMetric = collectServiceMetric(cluster.getClusterId());//收集服务监控信息
				metric.put("serviceMetric", serviceMetric);
				
				JSONArray serviceModeMetric = collectServiceModeMetric(cluster.getClusterId());//收集服务模块监控信息
				metric.put("serviceModeMetric", serviceModeMetric);
				
				collect.add(metric);
			}
			saveMetric(collect.toString());//写入文件
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		*/
		logger.info("end collect...");
	}
	
	/**
	private JSONArray collectHostMetric(String clusterId) throws Exception{
		Map<String,String> hostParamMap = new HashMap<String,String>();
		hostParamMap.put("cluster", clusterId);
		hostParamMap.put("hasMetrics", "Y");
		hostParamMap.put("hasServiceModes", "Y");
		
		QueryDTO<HostDTO> queryDto = hostMntService.findAll(hostParamMap);
		logger.info("collect host num : " + queryDto.getList().size());
		
		List<HostMonitorDTO> monitorList = 	hostMonitorService.findChartMetaData();
		logger.info("collect monitor num : " + monitorList.size());
		Map<String,String> monitorParamMap = new HashMap<String,String>();
		monitorParamMap.put("timeFlag", "4");
					
		JSONArray arr = new JSONArray();
		for(HostDTO host : queryDto.getList()){
			JSONObject obj = JSONObject.fromObject(host);
			List<HostMonitorDTO> metricList = 	hostMonitorService.findMetrics(host.getHostId());
			if(metricList != null && metricList.size() > 0){
				JSONArray monitorObj = JSONArray.fromObject(metricList);
				obj.put("metric", monitorObj);
			}
			
			monitorParamMap.put("hostId", host.getHostId());
			JSONArray monArr = new JSONArray();
			for(HostMonitorDTO monitor : monitorList){
				monitorParamMap.put("monitorId", monitor.getMonitorId());
				List<HostMonitorDTO> chartData = hostMonitorService.findChartData(monitorParamMap);
				monArr.add(JSONArray.fromObject(chartData));
			}
			obj.put("chardata", monArr);
			
			arr.add(obj);
		}
		
		return arr;
	}
	
	private JSONArray collectServiceMetric(String clusterId) throws Exception{
		List<ServiceDTO> services = serviceMntService.findService(clusterId, "");
		logger.info("collect service num : " + services.size());
					
		JSONArray arr = new JSONArray();
		for(ServiceDTO service : services){
			JSONObject obj = JSONObject.fromObject(service);
//			List<ServiceMonitorDTO> serviceMetricList = serviceMonitorService.findMetrics(service.getServiceId(),service.getServiceType());
//			if(serviceMetricList != null && serviceMetricList.size() > 0){
//				JSONArray monitorObj = JSONArray.fromObject(serviceMetricList);
//				obj.put("metric", monitorObj);
//			}
			
			List<ServiceMonitorDTO> serviceMonitorList = serviceMonitorService.findChartMetaData(service.getServiceType());
			logger.info("collect service monitor num : " + serviceMonitorList.size());
			Map<String,String> monitorParamMap = new HashMap<String,String>();
			monitorParamMap.put("timeFlag", "4");
			monitorParamMap.put("clusterId", clusterId);
			monitorParamMap.put("serviceId", service.getServiceId());
			monitorParamMap.put("serviceType", service.getServiceType());
			JSONArray monArr = new JSONArray();
			for(ServiceMonitorDTO monitor : serviceMonitorList){
				monitorParamMap.put("monitorId", monitor.getMonitorId());
				List<ServiceMonitorDTO> retChart = serviceMonitorService.findChartData(monitorParamMap);
				monArr.add(JSONArray.fromObject(retChart));
			}
			obj.put("chardata", monArr);
			
			arr.add(obj);
		}
		
		return arr;
	}
	
	private JSONArray collectServiceModeMetric(String clusterId) throws Exception{
		Map<String,String> serviceModeParamMap = new HashMap<String,String>();
		serviceModeParamMap.put("clusterId", clusterId);
		List<ServiceModeDTO> serviceModes = serviceMntService.findServiceMode(serviceModeParamMap);
		logger.info("collect service mode num : " + serviceModes.size());

		JSONArray arr = new JSONArray();
		for(ServiceModeDTO serviceMode : serviceModes){
			JSONObject obj = JSONObject.fromObject(serviceMode);
//			List<ServiceMonitorDTO> serviceMetricList = serviceMonitorService.findMetrics(serviceMode.getServiceId(),serviceMode.getServiceType());
//			if(serviceMetricList != null && serviceMetricList.size() > 0){
//				JSONArray monitorObj = JSONArray.fromObject(serviceMetricList);
//				obj.put("metric", monitorObj);
//			}
		
			List<ServiceMonitorDTO> serviceModeMonitorList = serviceModeMonitorService.findChartMetaData(serviceMode.getServiceModeType());
			logger.info("collect service mode monitor num : " + serviceModeMonitorList.size());
			Map<String,String> monitorParamMap = new HashMap<String,String>();
			monitorParamMap.put("timeFlag", "4");
			monitorParamMap.put("serviceModeId", serviceMode.getServiceModeId());
			JSONArray monArr = new JSONArray();
			for(ServiceMonitorDTO monitor : serviceModeMonitorList){
				monitorParamMap.put("monitorId", monitor.getMonitorId());
				List<ServiceMonitorDTO> retChart = serviceModeMonitorService.findChartData(monitorParamMap);
				monArr.add(JSONArray.fromObject(retChart));
			}
			obj.put("chardata", monArr);
			
			arr.add(obj);
		}
		
		return arr;
	}
	*/
	
	private void saveMetric(String content){
		FileOutputStream stream = null;
		try {
			File file = new File(collectMetricPath + File.separator + "metric." + currentFileNo);
			if(!file.exists()){
				boolean createRes = file.createNewFile();
				if(createRes == false){
					logger.error("create new file failed : " + file.getAbsolutePath());
					return;
				}
			}
			else if(file.length() >= (maxFileSize * 1024)){
				int nextFileNo = (currentFileNo + 1) % fileNum;
				currentFileNo = nextFileNo;
				file = new File(collectMetricPath + File.separator + "metric." + nextFileNo);
				if(file.exists()){
					file.delete();
				}
				boolean createRes = file.createNewFile();
				if(createRes == false){
					logger.error("create new file failed : " + file.getAbsolutePath());
					return;
				}
			}

			logger.info("collect write data into file : " + file.getAbsolutePath());
			stream = new FileOutputStream(file, true);
			stream.write((DateFormat.getDateTimeInstance().format(new Date()) + " : " + content + "\n").getBytes(Charset.forName("utf-8")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				if(stream != null)
					stream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}
