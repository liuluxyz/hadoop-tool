package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author liulu5
 *
 */
public class ReadFileHelper {

	private static final Log log = LogFactory.getLog(ReadFileHelper.class);
	
//	private boolean isLocal;

	public static String[] readLocalFileByLine(String filePath){
		List<String> lines = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			File file = new File(filePath);
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = reader.readLine()) != null){
				if(!"".equals(line.trim())){
					lines.add(line);	
				}
			}
		} catch (IOException e) {
			log.error("readLocalFileByLine error : ", e);
			e.printStackTrace();
		}finally{
			try {
				if(reader != null){
					reader.close();	
				}
			} catch (IOException e) {
				e.printStackTrace();
				log.error("readLocalFileByLine error : ", e);
			}
		}
		return lines.toArray(new String[0]);
	}
	
	public static String readLocalFile(String filePath){
		StringBuffer str = new StringBuffer();
		BufferedReader reader = null;
		try {
			File file = new File(filePath);
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = reader.readLine()) != null){
				if(!"".equals(line.trim())){
					str.append(" ").append(line.trim());
				}
			}
		} catch (IOException e) {
			log.error("readLocalFile error : ", e);
		}finally{
			try {
				if(reader != null){
					reader.close();	
				}
			} catch (IOException e) {
				log.error("readLocalFileByLine error : ", e);
			}
		}
		return str.toString();
	}
	
	public void writeAppend(String content) throws Exception{
		System.out.println("not support");
		throw new Exception("not support this function");
	}
	
}

