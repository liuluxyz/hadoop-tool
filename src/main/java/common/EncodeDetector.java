package common;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import cpdetector.io.ASCIIDetector;
import cpdetector.io.ByteOrderMarkDetector;
import cpdetector.io.CodepageDetectorProxy;
import cpdetector.io.JChardetFacade;
import cpdetector.io.ParsingDetector;
import cpdetector.io.UnicodeDetector;

/**
 * 检测文件的编码
 * liulu5
 * 2013-12-16
 */
public class EncodeDetector {

	private static boolean init = false;

	private static CodepageDetectorProxy detector = CodepageDetectorProxy.getInstance();

	private static ParsingDetector parsingDetector = new ParsingDetector(false);

	private static ByteOrderMarkDetector byteOrderMarkDetector = new ByteOrderMarkDetector();
	
	public static String getEncoding(BufferedInputStream buffIn) throws Exception{

	    int size = buffIn.available();
	    buffIn.mark(size);
	    CodepageDetectorProxy detector = getDetector();

	    java.nio.charset.Charset charset = null;
	    charset = detector.detectCodepage(buffIn, size);
	    buffIn.reset();
	    return charset.toString();

	}

	/**
	 * 获取二进制数组编码
	 * @param byteArr 数据数组
	 * @return 编码字符串
	 * @throws Exception
	 */
	public static String getEncoding(byte[] byteArr) throws Exception{

	    ByteArrayInputStream byteArrIn = new ByteArrayInputStream(byteArr);
	    BufferedInputStream buffIn = new BufferedInputStream(byteArrIn);
	    CodepageDetectorProxy detector = getDetector();
	    java.nio.charset.Charset charset = null;
	    charset = detector.detectCodepage(buffIn, buffIn.available());

	    try {
	        buffIn.close();
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }

	    return charset.toString();
	}

	private static CodepageDetectorProxy getDetector(){
	    if(!init){
	        detector.add(JChardetFacade.getInstance());
	        detector.add(ASCIIDetector.getInstance());
	        detector.add(UnicodeDetector.getInstance());
	        detector.add(parsingDetector);
	        detector.add(byteOrderMarkDetector);
	        init = true;
	    }
	    return detector;
	}
	
	public static void main(String[] args) {
		try {
			InputStream in = new FileInputStream("D:\\workspace\\workspace_tools\\test\\src\\test_filecode\\programinfo_20140710.dat");
			BufferedInputStream stream = new BufferedInputStream(in);
			String code = EncodeDetector.getEncoding(stream);
			System.out.println(code);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}

