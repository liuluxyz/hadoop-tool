package tmp.mapred;

import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 定制hadoop进度条
 * @author weijia
 *
 */
public class CustomerProcess {
	private Log log = LogFactory.getLog(CustomerProcess.class);
	private TaskReporter tr;

	@SuppressWarnings("unchecked")
	public CustomerProcess(Context context) {
		try {
			Field field = Context.class.getSuperclass().getSuperclass()
					.getDeclaredField("reporter");
			field.setAccessible(true);
			tr = (TaskReporter) field.get(context);
		} catch (Exception e) {
			log.error(e);
//			throw new AICloudETLRuntimeException(e);
		}
	}

	public void setProcess(float process) {
		tr.setProgress(process);
	}

}
