package common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 
 * @author liulu5
 *
 */
public class WriteFileHelper {

	private boolean isLocal;
	private String filePath;
	
	public WriteFileHelper(){
		this.isLocal = true;
		filePath = SimpleDateFormat.getDateTimeInstance().format(new Date());
	}
	
	public WriteFileHelper(boolean isLocal){
		this.isLocal = isLocal;
		if(isLocal == true){
			filePath = "./temp-" + SimpleDateFormat.getDateTimeInstance().format(new Date());
		}else{
			filePath = "/temp-" + SimpleDateFormat.getDateTimeInstance().format(new Date());
		}
	}
	
	public WriteFileHelper(boolean isLocal, String filePath){
		this.isLocal = isLocal;
		this.filePath = filePath;
	}
	
	public void writeOverwrite(String content){
		FileWriter write = null;
		try {
			File file = new File(filePath);
			File parent = file.getParentFile();
			if(!parent.exists()){
				if(!parent.mkdirs()){
					System.out.println("创建文件夹失败 : " + parent.getAbsolutePath());
				}
			}
			
//			file.deleteOnExit();
			if(!file.createNewFile()){
				System.out.println("创建文件失败 : " + file.getAbsolutePath());
			}

			write = new FileWriter(file);
			write.write(content);
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}finally{
			if(write != null)
				try {
					write.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
		}
	}
	
	public void writeAppend(String content) throws Exception{
		System.out.println("not support");
		throw new Exception("not support this function");
	}
	
}

