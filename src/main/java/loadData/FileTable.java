package loadData;

/**
 * liulu5
 * 2013-12-13
 */
public class FileTable {

	private boolean isLoad = true;
	private boolean isTableExist = false;
	private String filename;
	private String tablename;
	
	public FileTable(){
		
	}
	
	public FileTable(String filename, String tablename){
		this.filename = filename;
		this.tablename = tablename;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public boolean isLoad() {
		return isLoad;
	}

	public void setLoad(boolean isLoad) {
		this.isLoad = isLoad;
	}

	public boolean isTableExist() {
		return isTableExist;
	}

	public void setTableExist(boolean isTableExist) {
		this.isTableExist = isTableExist;
	}

	
}

