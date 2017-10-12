package common.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * liulu5
 * 2014-5-29
 */
public class ZipFileFormater {

	/**
	 * 解析zip压缩文件解压后的根目录名
	 * @param file
	 * @return
	 * @throws Exception
	 */
    public static String[] parseRootDir(String file) throws Exception{
    	List<String> dirs = new ArrayList<String>();
        try {
            ZipFile zip = new ZipFile(file);//由指定的File对象打开供阅读的ZIP文件
            Enumeration<ZipEntry> entries = (Enumeration<ZipEntry>) zip.entries();//获取zip文件中的各条目（子文件）
            while(entries.hasMoreElements()){//依次访问各条目
                ZipEntry ze = (ZipEntry) entries.nextElement();
//                if(!ze.isDirectory()){
//                	continue;
//                }
                
                String fileName = ze.getName();
//                System.out.println(fileName);
//                if(fileName.indexOf("/") < fileName.length()-1){
//                	continue;
//                }
                String dir = fileName.substring(0, fileName.indexOf("/"));
                if(!dirs.contains(dir)){
                    dirs.add(dir);              	
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return dirs.toArray(new String[0]);
    }  
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			String[] dirs = parseRootDir("C:\\Users\\liulu5\\Desktop\\temp.zip");
			System.out.println(Arrays.toString(dirs));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

