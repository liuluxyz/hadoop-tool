package common;

import java.text.DecimalFormat;

/**
 * liulu5
 * 2014-3-13
 */
public class FilesizeFormater {

	DecimalFormat df = new DecimalFormat("###.##");
	
	final static String[] units = new String[]{"B","KB","MB","GB","TB","PB"};
	
	final static double mod = 1024.0;
	
	public static String format(double size){
		for(int i=units.length; i>0; i--){
			if(size >= Math.pow(mod, i)){
				return (Math.round((size / Math.pow(mod, i)) * 100) / 100.0) + units[i-1];
			}
		}
		return size + "B";
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println(format(1024D * 1024D * 1024D * 1024D * 1024D * 1002.145d));
		
//		double x = 1024 * 1024 * 1024 * 1024D;
//		System.out.println(x >= Double.MAX_VALUE);
//		System.out.println(1024D * 1024D * 1024D * 1024D);
//		
//		System.out.println(1024D * 1024D * 1024D * 1024D * 2D);
		
//		System.out.println(Math.pow(mod, 3));
	}

}

