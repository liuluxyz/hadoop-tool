package analysejob.common.hql;

/**
 * liulu5
 * 2013-12-30
 */
public class Condition {

	private Object left;
	private Object right;
	private Table leftBelong;
	private Table rightBelong;
	
	public Object getLeft() {
		return left;
	}
	public void setLeft(Object left) {
		this.left = left;
	}
	public Table getLeftBelong() {
		return leftBelong;
	}
	public void setLeftBelong(Table leftBelong) {
		this.leftBelong = leftBelong;
	}
	public Table getRightBelong() {
		return rightBelong;
	}
	public void setRightBelong(Table rightBelong) {
		this.rightBelong = rightBelong;
	}
	public Object getRight() {
		return right;
	}
	public void setRight(Object right) {
		this.right = right;
	}
	
	@Override
	public String toString() {
		return "{[left:" + left + "] [right:" + right + "]}";
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

