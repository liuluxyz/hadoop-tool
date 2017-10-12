package autodeploy.model;

import java.util.Map;

import autodeploy.common.ServiceEnum;
import autodeploy.config.DeployConfig;

/**
 * liulu5
 * 2014-7-2
 */
public interface DistributedModel extends Comparable{

	public void parseService(DeployConfig config, Map<ServiceEnum, String> serviceHome) throws Exception;
	
	public void update() throws Exception;
	
	public void prepare() throws Exception;
	
	public void start() throws Exception;
	
	public void check() throws Exception;
	
	public ServiceEnum selfService();
}

