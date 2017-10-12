package common.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * shell文件修改
 * @author liulu5
 *
 */
public class ShellFileParser {

	public static void update(String filePath, Map<String, String> nodes)throws Exception{
		
//		sed -n '/^export \{1,\}JAVA_HOME=*/p' test
		
//		sed -i 's/export JAVA_HOME=\/home\/ocetl\/app\/java/export test/g' test
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Map<String, String> nodes = new HashMap<String, String>();
		nodes.put("dfs.nameservices", "ocdccluster");
		nodes.put("dfs.namenode.rpc-address.ocdccluster.nn2", "OCDC-NAME-002:8020");
		nodes.put("dfs.namenode.shared.edits.dir", "qjournal://OCDC-DATA-003:8487;OCDC-DATA-005:8487;OCDC-DATA-006:8487;OCDC-DATA-007:8487;OCDC-DATA-008:8487/ocdccluste");
		
		String file = "D:\\workspace\\workspace_tools\\hadoop_tool\\src\\common\\file\\core-site.xml";
//		try {
//			writePropertyElement(file, nodes);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
