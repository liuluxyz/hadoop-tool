package analysejob.common.hql;

/**
 * @author liulu5
 */
public class Table {

	String name;
	String alias;
	
	public Table(){
		
	}
	public Table(String name){
		this.name = name;
	}
	public Table(String name, String alias){
		this.name = name;
		this.alias = alias;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public String getWholeName(){
		return this.name + " " + this.alias;
	}
	
	@Override
	public String toString() {
		return "{[name:" + name + "] [alias:" + alias + "]}";
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Table){
			return this.getName().equals(((Table)obj).getName()) && this.getAlias().equals(((Table)obj).getAlias());
		}
		return false;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String n = JoinBean.joinRule.FULLOUTERJOIN.toString();
//		System.out.println(n);
	}

}

