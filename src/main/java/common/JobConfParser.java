package common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * 解析job的conf信息
 * liulu5
 * 2013-12-24
 */
public class JobConfParser {

	public static Map<String, String> parse(String filePath, boolean isLocal, FileSystem fs) throws Exception{
		Map<String, String> conf = new HashMap<String, String>();
		
		InputStream in;
		if(isLocal == true){
			if(!new File(filePath).exists()){
				System.out.println("filePath not exist : " + filePath);
				return conf; 
			}
			in = new FileInputStream(filePath);
		}else{
			in = fs.open(new Path(filePath));
		}
		
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dbBuilder = dbFactory.newDocumentBuilder();
        Document doc = dbBuilder.parse(in);
        NodeList list = doc.getElementsByTagName("property");
        for(int i = 0; i< list.getLength() ; i ++){
            Element element = (Element)list.item(i);
            String name = element.getElementsByTagName("name").item(0).getFirstChild().getNodeValue();
            String value = element.getElementsByTagName("value").item(0).getFirstChild().getNodeValue();
            conf.put(name, value);
        }
        return conf;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("C:\\Users\\liulu5\\Desktop\\analyse\\core-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] files = fs.listStatus(new Path("/"));
			
			
			JobConfParser.parse("D:\\workspace\\workspace_tools\\hadoop_tool\\src\\common\\job.xml", true, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

