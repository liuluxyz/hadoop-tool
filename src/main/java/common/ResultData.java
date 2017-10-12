package common;

/**
 * liulu5
 * 2013-12-16
 */
public class ResultData {

	private boolean result;
	private String message;
	
	public ResultData(){
		
	}
	
	public ResultData(boolean result){
		this.result = result;
	}
	
	public ResultData(boolean result, String message){
		this.result = result;
		this.message = message;
	}

	public boolean isResult() {
		return result;
	}


	public void setResult(boolean result) {
		this.result = result;
	}


	public String getMessage() {
		return message;
	}


	public void setMessage(String message) {
		this.message = message;
	}

}

