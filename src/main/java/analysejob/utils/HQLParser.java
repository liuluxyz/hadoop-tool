package analysejob.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import analysejob.common.hql.Condition;
import analysejob.common.hql.HQLBean;
import analysejob.common.hql.JoinBean;
import analysejob.common.hql.Table;

/**
 * liulu5
 * 2014-1-1
 * 
 * bug1: 解析join时的正则，需要先拿出hql中的最小单元，以括号为准，比如子查询中的join
 * bug2: 解析join时，若有多个join，则join的左右表不一定是from表，有可能是join后的表与另一个join表进行join，
 * 		  需要根据on条件中的字段所属表，将此join的双方确定
 * bug3: 解析各种join格式
 * bug4: 解析condition时，可以解析各种关联关系,不止等式
 * 
 */
public class HQLParser {

	private static final Log log = LogFactory.getLog(HQLParser.class);
	
	/**
	 * 将hql解析为bean
	 * @param hql
	 * @return
	 */
	public static HQLBean parseHQL(String hql){
		HQLBean hqlBean = new HQLBean();
		
		List<JoinBean> joins = parseFromJoin(hql);
		
		hqlBean.setJoins(joins);
		return hqlBean;
	}
	
	/**
	 * parse from join like 'select * from a left outer join b on a.id=b.id'
	 * @param fromJoinContent
	 * @return
	 */
	public static List<JoinBean> parseFromJoin(String fromJoinContent){
		log.info("start parse fromjoin : " + fromJoinContent);
		
		String[] joinStrs = fromJoinContent.toUpperCase().split("LEFT OUTER JOIN");
		if(joinStrs.length < 2){
			return null;
		}
//		String regex = "((?s).*)from((?s).*)left outer join((?s).*)";//暂时只解析【left outer join】格式
//		Pattern pattern = Pattern.compile(regex);
//		Matcher matcher = pattern.matcher(fromJoinContent.toLowerCase());

		Table fromTable = parseFromTable(joinStrs[0]);
		if(fromTable == null){
			log.warn("解析from表失败 : " + joinStrs[0]);
			return null;
		}
		log.info("parse from table : " + fromTable);
		
		List<Table> tables = new ArrayList<Table>();
		tables.add(fromTable);
		List<JoinBean> joins = new ArrayList<JoinBean>();
		for(int i=1; i<joinStrs.length; i++){
			JoinBean join = parseJoin(tables, joinStrs[i]);
			joins.add(join);
			if(!tables.contains(join.getLeftTable()))
				tables.add(join.getLeftTable());
			if(!tables.contains(join.getRightTable()))
				tables.add(join.getRightTable());
		}
		
		log.info("end parse fromjoin : " + joins.size());
		return joins;
	}
	
//	public static List<JoinBean> parseJoin(Table fromTable, String joinContent){
//		List<Table> tables = new ArrayList<Table>();
//		tables.add(fromTable);
//		return parseJoin(tables, joinContent);
//	}
	
	/**
	 * 解析join表达式
	 * @param hql
	 * @return
	 */
	public static JoinBean parseJoin(final List<Table> tables, String joinContent){
		log.info("start parse join : " + joinContent);
		String regex = "((?s).*)ON((?s).*)";//暂时只解析【left outer join】格式
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(joinContent.toUpperCase());
		
		JoinBean joinBean = new JoinBean();
		while(matcher.find()){
			//right table
			String rightTableStr = matcher.group(1);
			Table rightTableBean = HQLParser.parseTable(rightTableStr);
			if(rightTableBean == null){
				log.warn("解析join右表失败 : " + rightTableStr);
				return null;
			}
			joinBean.setRightTable(rightTableBean);
			tables.add(rightTableBean);
			log.info("parse right table : " + rightTableBean);
			
			//condition
			String conditionsStr = matcher.group(2);
			List<Condition> conditions = HQLParser.parseCondition(tables, conditionsStr);
			if(conditions == null || conditions.size() == 0){
				log.warn("解析出0个条件 : " + conditionsStr);
			}else{
				joinBean.setConditions(conditions);
				log.info("parse conditions : " + Arrays.toString(conditions.toArray(new Condition[0])));
			}
			
			//left table
			for(Condition condition : conditions){
				if(condition.getLeftBelong() != null && !rightTableBean.equals(condition.getLeftBelong())){
					joinBean.setLeftTable(condition.getLeftBelong());
				}
				if(condition.getRightBelong() != null && !rightTableBean.equals(condition.getRightBelong())){
					joinBean.setLeftTable(condition.getRightBelong());
				}
			}
			log.info("parse left table : " + joinBean.getLeftTable());
		}
		
		log.info("end parse join");
		return joinBean;
	}
	
	/**
	 * parse from content like 'select xxx from a'
	 * @param fromContent
	 * @return
	 */
	public static Table parseFromTable(String fromContent){
		log.info("start parse from table : " + fromContent);
		
		String str = fromContent.toUpperCase();
		
		if(str == null || "".equals(str)){
			log.warn("parse from table warn : str is null");
			return null;
		}
		if(!str.contains("FROM")){
			log.warn("parse from table warn : str contain no 'from'");
			return null;
		}
		
		String tableStr = str.substring(str.indexOf("FROM") + 4);
		
		Table table = parseTable(tableStr);
		
		log.info("end parse from table : " + table.toString());
		return table;
	}
	
	/**
	 * 将HQL中的【表】表达式解析为bean
	 * @param str
	 * @return
	 */
	public static Table parseTable(String str){
		log.info("start parse table : " + str);
		
		if(str == null || "".equals(str)){
			log.warn("parse table warn : str is null");
			return null;
		}
		Table table = new Table();
		
		String temp = str.toUpperCase().trim();
		int index = temp.lastIndexOf(" ");
		if(index == -1){//无alias
			table.setAlias("");
			table.setName(temp);
		}
		else{
			String name = temp.substring(0, index + 1).trim();
			String alias = temp.substring(index, temp.length()).trim();
			table.setAlias(alias);
			table.setName(name);
		}
		
		log.info("end parse table : " + table.toString());
		return table;
	}
	
	/**
	 * 将HQL中的条件表达式解析为bean
	 * @param str
	 * @return
	 */
	public static List<Condition> parseCondition(final List<Table> tables, String str){
		log.info("start parse condition : " + str);
		
		if(str == null || "".equals(str)){
			return null;
		}
		
		String temp = str.toUpperCase().trim();
		if(temp.startsWith("(") && temp.endsWith(")")){
			temp = temp.substring(1, temp.length() - 1);//先去除括号，再解析
		}
		else if(!temp.startsWith("(")){
			//正常解析
		}
		else{
			return null;//括号不匹配，错误
		}
		
		List<Condition> conditions = new ArrayList<Condition>();
		String[] cons = temp.split("AND");
		for(String con : cons){
			Condition condition = new Condition();
			if(con.indexOf("=") == -1){//暂时只解析等式
				return null;
			}
			String left = con.split("=")[0].trim();
			String right = con.split("=")[1].trim();
			for(Table table : tables){//检测条件的field所属的table
				if(left.contains(table.getAlias() + ".")){
					condition.setLeftBelong(table);
				}
				if(right.contains(table.getAlias() + ".")){
					condition.setRightBelong(table);
				}
			}
			condition.setLeft(left);
			condition.setRight(right);
			conditions.add(condition);
		}
		log.info("end parse condition : " + Arrays.toString(conditions.toArray(new Condition[0])));
		return conditions;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

//		parseCondition(null, "TRIM(A.PRODUCT_ID)=TRIM(D.BOSS_VALUE)");
		
//		String hql = "insert overwrite directory '/liulu/out' select a1.id, a2.name " +
//				"from a a1 left outer join a a2 on a1.name = a2.name";
		
		/**
	 	INPUT_FILE_NAME__ filename,
datatime datatime
FROM
trans_ods_cust_newmsg_ds_temp_1 t1
LEFT OUTER JOIN Map_CityCounty t2 ON (
upper(
	substr(
		field(trim(t1.OWNERAREAID), '.', 3),
		2,
		1
	)
) = upper(t2.OLD_COUNTY)
)
LEFT OUTER JOIN ODS_ORGANIZATION_MSG_DS_SUB t3 ON (
trim(t1.OWNERAREAID) = t3.channel_id
)


LEFT OUTER JOIN usys_etl_map_total t4 ON (
t4.map_name = 'FMAP_CUST_IDEN'
AND upper(trim(t1.CERTTYPE)) = upper(t4.boss_value)
)


LEFT OUTER JOIN usys_etl_map_total t5 ON (
t5.map_name = 'FMAP_PUB_STATUS'
AND upper(trim(t1. STATUS)) = upper(t5.boss_value)
)		 
	 */
		
//		String hql = "select t1.a, t2.b, t3.c, t4.d, t5.e " +
//				"FROM table-A t1 " +
//				"LEFT OUTER JOIN table-B t2 ON (t1.OWNERAREAID = t2.OLD_COUNTY) " +
//				"LEFT OUTER JOIN table-C t3 ON (t1.OWNERAREAID = t3.channel_id) " +
//				"LEFT OUTER JOIN table-D t4 ON (t4.map_name = 'FMAP_CUST_IDEN' AND t1.CERTTYPE = t4.boss_value) " +
//				"LEFT OUTER JOIN table-E t5 ON (t5.map_name = 'FMAP_PUB_STATUS' AND upper(trim(t2.STATUS)) = upper(t5.boss_value))";
		
//		String hql = "INSERT OVERWRITE TABLE tmp_dw_cust_extmsg_ds select '2014-01-19' OP_TIME, a.city_id, a.county_id, b.town_id, a.user_id, a.product_no, a.cust_id, c.area_attribute, a.channel_id, d.channeltype_id, case when d.channeltype_id in (1301,1302,1303,3101) or d.channeltype_id=3102 and empty(d.NETWORKTYPE,'none')!='none' then 1 else 0 end boss_conn, case when d.channeltype_id in (1100,1200,1301,1302,1303,2101,2102,2200,2300) then 1 else 0 end self_run, 0 newcust_num, 0 newcust_lvl, a.userstatus_id, a.brand_id, a.product_id, f.plan_pay_mode, e.iden_id, e.iden_no, e.cust_name, e.cust_address, case when scale(e.cust_name)>=4 and scale(e.cust_address)>=4 and e.iden_id<>0 and length(e.iden_no)>=4 then 1 else 0 end msg_complete, 0 iden_custnum, 0 iden_custnum_lvl, a.user_online_id, a.cust_online_id, a.enterprise_id, case when a.enterprise_id is not null and length(a.enterprise_id)>1 then 1 else 0 end enterprise_flag, a.open_date user_opentime, e.cust_opentime from (select * from dw_product_dt where pt_time_='2014-01-19' and userstatus_id between 0 and 99) a left outer join( select * from dw_user_town_ds where pt_time_='2014-01-19') b on (a.user_id=b.user_id) left outer join (select * from (select *, row_number() over (distribute by user_id sort by user_id desc) as rn from dw_product_area_static_attribute where pt_time_='2014-01-19') dd where dd.rn=1 ) c on (a.user_id=c.user_id) left outer join (select * from (select channel_id,channeltype_id,NETWORKTYPE, row_number() over (distribute by channel_id sort by channel_id desc) as rn from ods_channel_msg_ds_monitor where pt_time_='2014-01-19') dd where dd.rn=1) d on (a.channel_id=d.channel_id) left outer join (select * from (select *, row_number() over (distribute by cust_id sort by cust_id desc) as rn from dw_cust_msg_ds where pt_time_='2014-01-19') dd where dd.rn=1) e on (a.cust_id=e.cust_id) left outer join (select * from dim_pub_product where trim(product_id)<>'' and product_id is not null) f on (a.product_id=f.product_id) ";
		
		String hql = "select t1.a, t2.b from test t1 left outer join test t2 on t1.b = t2.b";
		parseFromJoin(hql);
		
	}

}

