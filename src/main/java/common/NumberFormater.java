package common;

/**
 * liulu5
 * 2014-3-13
 */
public class NumberFormater {

	private final static double yi = 100000000;
	private final static double wan = 10000;
	
	public static String format(double num){
		
		if(num > yi){
			return (Math.round((num / yi) * 100) / 100.0) + "亿";
		}
		if(num > wan){
			return (Math.round((num / wan) * 100) / 100.0) + "万";
		}
		return num + "";
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println(format(93431823L));
	}

}

