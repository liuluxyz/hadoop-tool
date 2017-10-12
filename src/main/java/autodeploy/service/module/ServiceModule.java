package autodeploy.service.module;

/**
 * liulu5
 * 2014-6-16
 */
public interface ServiceModule {

	String name = null;
	
	public void start();
	
	public void stop();
	
}
