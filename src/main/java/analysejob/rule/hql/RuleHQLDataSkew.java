package analysejob.rule.hql;

import hivetool.HiveConnHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import analysejob.AnalyseHQLOption;
import analysejob.common.AnalyseResult;
import analysejob.common.hql.Condition;
import analysejob.common.hql.HQLBean;
import analysejob.common.hql.JoinBean;
import analysejob.common.hql.Table;
import analysejob.utils.HQLParser;

/**
 * @author liulu5
 */
public class RuleHQLDataSkew extends HQLAnalyseRule{

	private static final Log log = LogFactory.getLog(RuleHQLDataSkew.class);
	
	private final long recordsThreshold = 10000;//记录数倾斜阀值
	private final long cartesianProductThreshold = 10000000;//1千万:笛卡尔积的阀值
	
	@Override
	public AnalyseResult doAnalyse(AnalyseHQLOption option) throws Exception {
		log.info("start doAnalyse...");
		
//		AnalyseResult result = null;
		List<String> skewResult = new ArrayList<String>();
		HQLBean hqlBean = HQLParser.parseHQL(option.getHql());
		for(int i=0; i<hqlBean.getJoins().size(); i++){
			log.info("Join分析进度 : 第[" + (i+1) + "] of [" + hqlBean.getJoins().size() + "]");
			
			JoinBean join = hqlBean.getJoins().get(i);
			for(int j=0; j<join.getConditions().size(); j++){
				log.info("condition分析进度 : 第[" + (j+1) + "] of [" + join.getConditions().size() + "]");
				
				Condition con = join.getConditions().get(j);
				Table leftTable = join.getLeftTable();
				String leftField = con.getLeft().toString();
//				String leftFieldWithoutAlias = (leftTable.getAlias() == null) ? leftField : leftField.replaceAll(leftTable.getAlias() + "\\.", "");
				Table rightTable = join.getRightTable();
				String rightField = con.getRight().toString();
//				String rightFieldWithoutAlias = (rightTable.getAlias() == null) ? rightField : rightField.replaceAll(rightTable.getAlias() + "\\.", "");
				log.info("开始分析倾斜:左表[" + leftTable.getName()+ "]左表字段[" + leftField + "]右表[" + rightTable.getName() + "]右表字段[" + rightField + "]");
				
				//1. 直接检查是否有空值的倾斜
				log.info("start分析空值倾斜...");
				String queryEmptyLeft = "select " + leftField + ", count(1) num from " + leftTable.getWholeName() + " where " + leftField + " is null or " + leftField + " = '' group by " + leftField + "";	
				List<String[]> leftEmptyValues = HiveConnHelper.getInstance().executeQuery(option.getHiveserver(), option.getPort(), option.getDb(), queryEmptyLeft, 2);
				String queryEmptyRight = "select " + rightField + ", count(1) num from " + rightTable.getWholeName() + " where " + rightField + " is null or " + rightField + " = '' group by " + rightField + "";	
				List<String[]> rightEmptyValues = HiveConnHelper.getInstance().executeQuery(option.getHiveserver(), option.getPort(), option.getDb(), queryEmptyRight, 2);
				
				List<String> emptySkew = parseSkew(leftField, leftTable.getName(), rightField, rightTable.getName(), leftEmptyValues, rightEmptyValues);
				if(emptySkew.size() > 0){
//					result = new AnalyseResult(toString(), AnalyseResult.Result.needimprove, emptySkew.toArray(new String[0]), getReason(), getSuggestion());
					skewResult.addAll(emptySkew);
					log.info("空值倾斜分析结果:" + emptySkew.toArray(new String[0]));
					log.info("end分析空值倾斜...");
//					break;
				}
				log.info("end分析空值倾斜...");
				
				//2. 检查其他值是否有倾斜
				log.info("start分析未知值倾斜...");
				String queryLeft = "select analyse_temp_field, num from (select " + leftField + " as analyse_temp_field, count(1) num from " + leftTable.getWholeName() + " group by " + leftField + ") temp where num > 1000";
				List<String[]> leftValues = HiveConnHelper.getInstance().executeQuery(option.getHiveserver(), option.getPort(), option.getDb(), queryLeft, 2);
				String queryRight = "select analyse_temp_field, num from (select " + rightField + " as analyse_temp_field, count(1) num from " + rightTable.getWholeName() + " group by " + rightField + ") temp where num > 1000";
				List<String[]> rightValues = HiveConnHelper.getInstance().executeQuery(option.getHiveserver(), option.getPort(), option.getDb(), queryRight, 2);
				
				List<String> skew = parseSkew(leftField, leftTable.getName(), rightField, rightTable.getName(), leftValues, rightValues);
				if(skew.size() > 0){
//					result = new AnalyseResult(toString(), AnalyseResult.Result.needimprove, skew.toArray(new String[0]), getReason(), getSuggestion());
					skewResult.addAll(skew);
					log.info("未知值倾斜分析结果:" + Arrays.toString(skew.toArray(new String[0])));
					log.info("end分析未知值倾斜...");
//					break;
				}
			}
		}
		
		log.info("end doAnalyse...");
		if(skewResult.size() > 0){
			return new AnalyseResult(toString(), AnalyseResult.Result.needimprove, skewResult.toArray(new String[0]), getReason(), getSuggestion());
		}
		return new AnalyseResult(toString(), AnalyseResult.Result.noneedimprove, new String[0], getReason(), getSuggestion());
	}
	
	/**
	 * 解析数据倾斜和笛卡尔积
	 * @param leftField
	 * @param leftTableName
	 * @param rightField
	 * @param rightTableName
	 * @param leftValues
	 * @param rightValues
	 * @return
	 */
	private List<String> parseSkew(String leftField, String leftTableName, String rightField, String rightTableName, 
			List<String[]> leftValues, List<String[]> rightValues){
		
		String[][] leftValuesSorted = leftValues.toArray(new String[0][]);
		Arrays.sort(leftValuesSorted, new CountComparable());
		String[][] rightValuesSorted = rightValues.toArray(new String[0][]);
		Arrays.sort(rightValuesSorted, new CountComparable());
		
		int limit = 5;//检查倾斜值的个数限制,多了就无所谓倾斜了
		List<String> skew = new ArrayList<String>();
		for(int i=0; i<leftValuesSorted.length; i++){//检查左表是否有字段值倾斜
			if(Integer.parseInt(leftValuesSorted[i][1]) > recordsThreshold){
				String str = "表[" + leftTableName + "]中字段[" + leftField + "]数据倾斜，其中值为[" + leftValuesSorted[i][0] + "]的记录数为[" + leftValuesSorted[i][1] + "]";
				skew.add(str);
				log.info(str);
				if((i+1) >= limit){
					break;
				}
			}else{//已排好序，故可以break
				break;
			}
		}
		
		for(int i=0; i<rightValuesSorted.length; i++){//检查右表是否有字段值倾斜
			if(Integer.parseInt(rightValuesSorted[i][1]) > recordsThreshold){
				String str = "表[" + rightTableName + "]中字段[" + rightField + "]数据倾斜，其中值为[" + rightValuesSorted[i][0] + "]的记录数为[" + rightValuesSorted[i][1] + "]";
				skew.add(str);
				log.info(str);
				if((i+1) >= limit){
					break;
				}else{//已排好序，故可以break
					break;
				}
			}
		}
		
		for(String[] leftStrs : leftValuesSorted){//检查左右两表是否会产生笛卡尔积
			for(String[] rightStrs : rightValuesSorted){
				if((leftStrs[0] == null && rightStrs[0] == null) || leftStrs[0].equals(rightStrs[0])){
					long product = Long.parseLong(leftStrs[1]) * Long.parseLong(rightStrs[1]);
					if(product > cartesianProductThreshold){
						String str = "表[" + leftTableName + "]中字段[" + leftField + "]值为[" + leftStrs[0] + "]的记录数为[" + leftStrs[1] + 
								"],表[" + rightTableName + "]中字段[" + rightField + "]值为[" + leftStrs[0] + "]的记录数为[" + rightStrs[1] + 
								"],两个表因此产生笛卡尔积，笛卡尔积的结果记录数为[" + product + "]";
						skew.add(str);
						log.info(str);
					}
				}
			}
		}
		return skew;
	}

	@Override
	public String toString() {
		return "检查hql语句中join操作是否有数据倾斜";
	}
	
	@Override
	public String[] getDesc() {
		return new String[0];
	}

	@Override
	public String[] getReason() {
		return new String[0];
	}

	private static class CountComparable implements Comparator<String[]> {
		@Override
		public int compare(String[] o1, String[] o2) {
			if(o1 == null || o1.length != 2){
				return 1;
			}
			if(o2 == null || o2.length != 2){
				return 1;
			}
			return Integer.parseInt(o2[1]) - Integer.parseInt(o1[1]);
		}
	}
	
	@Override
	public String[] getSuggestion() {
		return new String[]{
				"若数据为非法数据，则过滤或者忽略倾斜数据",
				"若数据需要保留，则使用随机数进行替换"
		};
	}
	
	public static void main(String[] args) {
		String hql = "insert overwrite directory '/liulu/out' select a1.id, a2.name " +
				"from a a1 left outer join a a2 on a1.name = a2.name";
		
		String[][] a = new String[][]{
				new String[]{"a", "1"},
				new String[]{"b", "4"},
				new String[]{"c", "2"},
		};
		Arrays.sort(a, new CountComparable());
		
		for(String[] aa : a){
			System.out.println(aa[1]);
		}
		
	}
}

