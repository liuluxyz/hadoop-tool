package loadData;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * liulu5
 * 2013-12-13
 */
public class ReplaceConf {

	public static Map<String, String> get(){
		Map<String, String> replace = new LinkedHashMap<String, String>();
		replace.put("_2012\\.tdw1", ".tdw1");
		replace.put("20131212", "dm");
		replace.put("20131230", "dm");
		
		replace.put("201312", "mm");
		replace.put("_1_", "_");
		replace.put("_2_", "_");
		replace.put("_3_", "_");
		replace.put("_4_", "_");
		replace.put("_5_", "_");
		replace.put("_6_", "_");
		replace.put("_7_", "_");
		replace.put("_8_", "_");
		replace.put("_9_", "_");
		replace.put("_a_", "_");
		replace.put("_b_", "_");
		replace.put("_c_", "_");
		replace.put("_d_", "_");
		replace.put("_10_", "_");
		replace.put("_11_", "_");
		replace.put("_12_", "_");
		replace.put("_13_", "_");
		replace.put("_14_", "_");
		replace.put("_15_", "_");
		replace.put("_16_", "_");
		replace.put("_17_", "_");
		replace.put("_18_", "_");
		replace.put("_19_", "_");
		replace.put("_0\\.", ".");
		replace.put("_1\\.", ".");
		replace.put("_2\\.", ".");
		replace.put("_3\\.", ".");
		/**
		replace.put("_01.", ".");
		replace.put("_02.", ".");
		replace.put("_03.", ".");
		replace.put("_04.", ".");
		replace.put("_05.", ".");
		replace.put("_06.", ".");
		replace.put("_07.", ".");
		replace.put("_08.", ".");
		replace.put("_09.", ".");
		replace.put("_10.", ".");
		replace.put("_11.", ".");
		replace.put("_21.", ".");
		replace.put("_22.", ".");
		replace.put("_650.", ".");
		replace.put("_651.", ".");
		replace.put("_652.", ".");
		replace.put("_653.", ".");
		replace.put("_654.", ".");
		replace.put("_655.", ".");
		replace.put("_656.", ".");
		replace.put("_657.", ".");
		replace.put("_658.", ".");
		replace.put("_890.", ".");
		*/
		for(int i=0; i<1000; i++){
			if(i < 10){
				replace.put("_" + i + "\\.", ".");
				replace.put("_0" + i + "\\.", ".");
			}else{
				replace.put("_" + i + "\\.", ".");
			}
		}
		
		String[] removeStr = new String[]{
				"_01", "_02", "_03", "_04", "_05", "_06", "_07", "_08", "_09", 
				"_2012\\.", 
				"_201202",
				"vgop\\.",
				"_preday"
		};
		for(String str : removeStr){
			replace.put(str, "");
		}
		
		return replace;
	}
	
	public static void main(String[] args) {
		String a = "cdr_gprs_20131211.tdw1.del";
		String b = a.replaceAll("_201\\.", ".");
		String c = a.replaceAll("_20131211\\.", ".");
		System.out.println(c);
	}

}

