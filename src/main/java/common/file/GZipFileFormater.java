package common.file;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.compress.utils.IOUtils;

/**
 * liulu5 2014-5-29
 */
public class GZipFileFormater {

	/**
	 * 解析tar和tar.gz压缩文件解压后的根目录名
	 * @param targzFile
	 * @return
	 * @throws Exception
	 */
	public static String[] parseRootDir(String targzFile) throws Exception{
		List<String> dirs = new ArrayList<String>();
		FileInputStream fileIn = null;
		BufferedInputStream bufIn = null;
		GZIPInputStream gzipIn = null;
		TarArchiveInputStream taris = null;
		try {
			fileIn = new FileInputStream(targzFile);
			bufIn = new BufferedInputStream(fileIn);
			gzipIn = new GZIPInputStream(bufIn); // first unzip the input file
			taris = new TarArchiveInputStream(gzipIn);
			
			TarArchiveEntry entry = null;
			while ((entry = taris.getNextTarEntry()) != null) {				
//                if(!entry.isDirectory()){
//                	continue;
//                }
                
                String fileName = entry.getName();
//                if(fileName.indexOf("/") < fileName.length()-1){
//                	continue;
//                }
                String dir = fileName.substring(0, fileName.indexOf("/"));
                if(!dirs.contains(dir)){
                    dirs.add(dir);              	
                }
			}
		}catch(Exception e){
			e.printStackTrace();
			throw e;
		}
		finally {
			try {
				taris.close();
				gzipIn.close();
				fileIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return dirs.toArray(new String[0]);
	}
		
//	public static void visitTARGZ(String targzFile) throws IOException {
//		FileInputStream fileIn = null;
//		BufferedInputStream bufIn = null;
//		GZIPInputStream gzipIn = null;
//		TarArchiveInputStream taris = null;
//		try {
//			fileIn = new FileInputStream(targzFile);
//			bufIn = new BufferedInputStream(fileIn);
//			gzipIn = new GZIPInputStream(bufIn); // first unzip the input file
//			// stream.
//			taris = new TarArchiveInputStream(gzipIn);
//			System.out.println(taris.getCount());
//			
//			TarArchiveEntry entry = null;
//			while ((entry = taris.getNextTarEntry()) != null) {
//				if (entry.isDirectory())
//					continue;
//				// configure(taris, ((TarArchiveEntry) entry).getFile());
//				// //process every entry in this tar file.
//				System.out.println(entry);
//				File file = entry.getFile();
//				// System.out.println(file.getName());
//				System.out.println(entry.getSize());
//				// FileInputStream fo = new FileInputStream(file);
//
//				byte[] b = new byte[(int) entry.getSize()];
//				// fo.read(b);
//				taris.read(b);
//				taris.read(b, 0, (int) entry.getSize());
//				System.out.println(b.length);
//				System.out.println(b);
//			}
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//		finally {
//			taris.close();
//			gzipIn.close();
//			bufIn.close();
//			fileIn.close();
//		}
//	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

//		String file = "D:\\project\\cdh4\\cdh4.2.1\\hive-0.10.0-cdh4.2.1.tar.gz";
//		String file = "D:\\project\\cdh4\\cdh5.0.0\\hive-0.12.0-cdh5.0.0.tar.gz";
		String file = "D:\\project\\cdh4\\cdh4.4.0\\hive-0.10.0-cdh4.4.0.tar.gz";
		
		String a = GzipUtils.getCompressedFilename(file);
		System.out.println(a);

		try {
			String[]dirs = parseRootDir(file);
			System.out.println(Arrays.toString(dirs));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
