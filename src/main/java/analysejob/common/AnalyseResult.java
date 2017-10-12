package analysejob.common;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * liulu5
 * 2013-11-22
 */
public class AnalyseResult {

	public static enum Result{
		fail, 
		needimprove, 
		noneedimprove;

		@Override
		public String toString() {
			switch(this){
			case fail:
				return "失败";
			case needimprove:
				return "需要优化";
			case noneedimprove:
				return "不需要优化";
			}
			return super.toString();
		}
	};
	
	String ruleName;
	Result result;
	String[] desc;
	String[] reason;
	String[] suggestion;
	Map<Object, Object> referInfo;
	
	public AnalyseResult(){
		
	}
	
	public AnalyseResult(String ruleName, Result result, String[] desc, String[] reason, String[] suggestion){
		this.ruleName = ruleName;
		this.result = result;
		this.desc = desc;
		this.reason = reason;
		this.suggestion = suggestion;
	}
	public AnalyseResult(String ruleName, Result result, String[] desc, String[] reason, String[] suggestion, Map<Object, Object> referInfo){
		this.ruleName = ruleName;
		this.result = result;
		this.desc = desc;
		this.reason = reason;
		this.suggestion = suggestion;
		this.referInfo = referInfo;
	}
	
	
	public String getRuleName() {
		return ruleName;
	}
	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}
	public Result getResult(){
		return result;
	}
	public void setResult(Result result) {
		this.result = result;
	}
	public String[] getSuggestion() {
		return suggestion;
	}
	public void setSuggestion(String[] suggestion) {
		this.suggestion = suggestion;
	}
	public String[] getDesc() {
		return desc;
	}
	public void setDesc(String[] desc) {
		this.desc = desc;
	}
	public String[] getReason() {
		return reason;
	}
	public void setReason(String[] reason) {
		this.reason = reason;
	}
	public Map<Object, Object> getReferInfo() {
		return referInfo;
	}
	public void setReferInfo(Map<Object, Object> referInfo) {
		this.referInfo = referInfo;
	}
	public void addReferInfo(Object key, Object value) {
		if(this.referInfo == null){
			this.referInfo = new LinkedHashMap<Object, Object>();
		}
		this.referInfo.put(key, value);
	}

	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		str.append("	规则: " + ruleName + "\r\n");
		str.append("	结果: " + result  + "\r\n");
		
		str.append("	说明: ");
		str.append("\r\n");
		for(int i=0; i<desc.length; i++){
			str.append("		" + (i+1) + ". " + desc[i]);
			str.append("\r\n");
		}
		
		str.append("	原因: ");
		str.append("\r\n");
		for(int i=0; i<reason.length; i++){
			str.append("		" + (i+1) + ". " + reason[i]);
			str.append("\r\n");
		}
		
		str.append("	建议: ");
		str.append("\r\n");
		for(int i=0; i<suggestion.length; i++){
			str.append("		" + (i+1) + ". " + suggestion[i]);
			str.append("\r\n");
		}
		
		str.append("	信息: ");
		str.append("\r\n");
		if(referInfo != null){
			Iterator<Object> it = referInfo.keySet().iterator();
			while(it.hasNext()){
				Object key = it.next();
				Object value = referInfo.get(key);
				str.append("		" + key + ": " + value + "\r\n");
			}	
		}
		return str.delete(str.length()-2, str.length()).toString();
	}

}

