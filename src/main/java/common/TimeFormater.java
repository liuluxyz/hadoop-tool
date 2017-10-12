package common;

/**
 * liulu5
 * 2014-3-6
 */
public class TimeFormater {

	public static String format(long milliSeconds){
		int day = (int) (milliSeconds / (1000 * 60 * 60 * 24));
		int hour = (int) ((milliSeconds % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
		int minute = (int) ((milliSeconds % (1000 * 60 * 60)) / (1000 * 60));
		long second = milliSeconds % (1000 * 60) / 1000;
		int msec = (int) (milliSeconds % 1000);
		
		if(day != 0){
			return day + "天 " + hour + "小时 " + minute + "分钟";
		}
		if(hour != 0){
			return hour + "小时 " + minute + "分钟 " + second + "秒";
		}
		if(minute != 0){
			return minute + "分钟 " + second + "秒 " + msec + "毫秒";
		}
		if(second != 0){
			return second + "秒 " + msec + "毫秒";
		}
		if(msec != 0){
			return msec + "毫秒";
		}
		return "";
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println(format(112344566));
	}

}

