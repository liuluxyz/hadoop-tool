package analysejob.rule.hql;

import analysejob.AnalyseHQLOption;
import analysejob.common.AnalyseResult;

/**
 * 
 * @author liulu5
 *
 */
public abstract class HQLAnalyseRule {
	
	public abstract AnalyseResult doAnalyse(AnalyseHQLOption option) throws Exception;
	
	public abstract String[] getDesc();
	
	public abstract String[] getReason();
	
	public abstract String[] getSuggestion();
	
}

