package getConf;

import org.apache.hadoop.conf.Configuration;

public class GetConf {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if(args.length == 0){
			System.out.println("input conf key");
			return;
		}
		Configuration conf = new Configuration();
		for(String arg : args){
			String value = conf.get(arg);
			System.out.println("key : " + arg + " ** value : " + value);
		}
		
		
		
	}

}
