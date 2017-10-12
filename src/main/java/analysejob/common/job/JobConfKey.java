package analysejob.common.job;


public class JobConfKey {

	public static final String dfsBlockSize 	= "dfs.blocksize";
	public static final String mrChildJavaOpts 	= "mapred.child.java.opts";
	
	public static final String jobName 			= "mapred.job.name";
	public static final String tclName 			= "tcl.name";
	
	public static final String hiveStageName 	= "mapreduce.workflow.node.name";
	public static final String hiveQueryString 	= "hive.query.string";
	
	public static final String mapredMaxSplitSize 			= "mapred.max.split.size";
	public static final String mapredMinSplitSize 			= "mapred.min.split.size";
	public static final String mapredMinSplitSizePerNode 	= "mapred.min.split.size.per.node";
	public static final String mapredMinSplitSizePerRack 	= "mapred.min.split.size.per.rack";
	public static final String hiveInputFormat 				= "hive.input.format";
	
	public static final String hiveExecReducersBytesPerReducer 	= "hive.exec.reducers.bytes.per.reducer";
	public static final String mapredReduceTasks 	= "mapred.reduce.tasks";
	
}
