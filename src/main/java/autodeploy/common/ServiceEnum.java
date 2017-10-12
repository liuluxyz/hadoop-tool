package autodeploy.common;

/**
 * liulu5
 * 2014-7-8
 */
public enum ServiceEnum{

	/** zookeeper、hdfs、mapreduce、yarn有依赖关系，决定部署时的顺序，故此处的enum顺序不可变 */
	
	java,
	zookeeper,
	hdfs,
	mapreduce,
	yarn,
	hive,
	hbase,
	spark
	
}

