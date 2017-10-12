package autodeploy.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import autodeploy.common.ServiceEnum;
import autodeploy.config.DeployConfig;
import autodeploy.model.impl.ha.HDFSHADistributedModel;
import autodeploy.model.impl.nonha.HDFSNonHADistributedModel;
import autodeploy.model.impl.nonha.MR1NonHADistributedModel;
import autodeploy.model.impl.nonha.YARNNonHADistributedModel;
import autodeploy.model.impl.nonha.ZookeeperDistributedModel;
import autodeploy.model.impl.pseudo.HDFSPseudoDistributedModel;
import autodeploy.model.impl.pseudo.MR1PseudoDistributedModel;
import autodeploy.model.impl.pseudo.YARNPseudoDistributedModel;
import autodeploy.model.impl.pseudo.ZookeeperPseudoDistributedModel;

/**
 * @Description: hadoop集群部署模式解析
 * @author liulu5
 * @date 2014-7-16 下午5:06:21 
 */
public class DistributedModelFactory {

	public static DistributedModel[] parseDistributedModel(DeployConfig config){
		ServiceEnum[] services = config.getDeployService();
		Arrays.sort(services);//排序，各个组件的部署有依赖关系,顺序以ServiceEnum中enum的顺序为准

		List<DistributedModel> models = new ArrayList<DistributedModel>();
		for(int i=0; i<services.length; i++){
			if(ServiceEnum.java.ordinal() == services[i].ordinal()){
				continue;//java不需要对应的DistributedModel实例
			}
			else if(ServiceEnum.hdfs.ordinal() == services[i].ordinal()){
				if(!config.isHdfsDistributed()){//伪分布式
					models.add(new HDFSPseudoDistributedModel());
				}
				else if(config.isNamenodeHA()){
					models.add(new HDFSHADistributedModel());
				}
				else if(config.isNamenodeFederation()){
					//...
				}
				else{//非HA的分布式
					models.add(new HDFSNonHADistributedModel());
				}
			}
			else if(ServiceEnum.mapreduce.ordinal() == services[i].ordinal()){
				if(!config.isMr1Distributed()){
					models.add(new MR1PseudoDistributedModel());
				}
				else if(config.isJobtrackerHA()){
					//...
				}
				else{//非HA的分布式
					models.add(new MR1NonHADistributedModel());
				}
			}
			else if(ServiceEnum.yarn.ordinal() == services[i].ordinal()){
				if(!config.isYarnDistributed()){
					models.add(new YARNPseudoDistributedModel());
				}
				else if(config.isResourcemanagerHA()){
					//...
				}
				else{//非HA的分布式
					models.add(new YARNNonHADistributedModel());
				}
			}
			else if(ServiceEnum.hive.ordinal() == services[i].ordinal()){
				
			}
			else if(ServiceEnum.zookeeper.ordinal() == services[i].ordinal()){
				if(!config.isZookeeperDistributed()){
					models.add(new ZookeeperPseudoDistributedModel());
				}
				else{//分布式
					models.add(new ZookeeperDistributedModel());
				}
			}
			else if(ServiceEnum.hbase.ordinal() == services[i].ordinal()){
				
			}
			else if(ServiceEnum.spark.ordinal() == services[i].ordinal()){
				
			}
		}
		Collections.sort(models);
		return models.toArray(new DistributedModel[0]);
	}
}

