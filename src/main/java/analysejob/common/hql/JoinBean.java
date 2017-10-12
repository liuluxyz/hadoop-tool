package analysejob.common.hql;

import java.util.List;

/**
 * liulu5
 * 2013-12-30
 */
public class JoinBean {

	private Table leftTable;
	private Table rightTable;
	List<Condition> conditions;
	
//	public enum joinRule{
//		JOIN,
//		LEFTOUTERJOIN,
//		RIGHTOUTERJOIN,
//		FULLOUTERJOIN,
//		CROSSJOIN;
//
//		@Override
//		public String toString() {
//			switch(this){
//			case JOIN:
//				return "JOIN";
//			case LEFTOUTERJOIN:
//				return "LEFT OUTER JOIN";
//			case RIGHTOUTERJOIN:
//				return "RIGHT OUTER JOIN";
//			case FULLOUTERJOIN:
//				return "FULL OUTER JOIN";
//			case CROSSJOIN:
//				return "CROSS JOIN";
//			}
//			return super.toString();
//		}
//	};
	
	public List<Condition> getConditions() {
		return conditions;
	}
	public Table getLeftTable() {
		return leftTable;
	}
	public void setLeftTable(Table leftTable) {
		this.leftTable = leftTable;
	}
	public Table getRightTable() {
		return rightTable;
	}
	public void setRightTable(Table rightTable) {
		this.rightTable = rightTable;
	}
	public void setConditions(List<Condition> conditions) {
		this.conditions = conditions;
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

