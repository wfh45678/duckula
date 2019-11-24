package net.wicp.tams.duckula.common.constant;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;

@Slf4j
public enum FilterPattern {
	regular("正则表达式"), sql("带参数SQL"), function("自定义函数，考虑使用脚本语言"), colname("列过滤");

	public static final String db_tb_formart = "%s|%s";
	public static final String ruleFormat = "duckula.busi.filter.%s.%s.%s.%s.%s=%s";

	private final String desc;

	// key:db_tb_formart value:{} key:field,value:Set<String> 规则值,容许多条
	/*
	 * private final Map<String, Map<String, Set<String>>> filterRules = new
	 * HashMap<String, Map<String, Set<String>>>();
	 * 
	 * public Map<String, Map<String, Set<String>>> getFilterRules() { return
	 * this.filterRules; }
	 */

	public static JSONArray getJson(Map<FilterPattern, Map<String, Map<String, Set<String>>>> inputmap) {
		JSONArray retall = new JSONArray();
		if (MapUtils.isEmpty(inputmap)) {
			return retall;
		}
		for (FilterPattern filterPattern : inputmap.keySet()) {
			JSONArray ret = new JSONArray();
			Map<String, Map<String, Set<String>>> filterRules = inputmap.get(filterPattern);
			for (String db_tb : filterRules.keySet()) {
				String[] db_tb_ary = db_tb.split("\\|");
				Map<String, Set<String>> map = filterRules.get(db_tb);
				for (String field : map.keySet()) {
					Set<String> ruleSet = map.get(field);
					if (CollectionUtils.isNotEmpty(ruleSet)) {
						String[] ruleAry = ruleSet.toArray(new String[ruleSet.size()]);
						for (int i = 0; i < ruleAry.length; i++) {
							JSONObject tempjson = new JSONObject();
							tempjson.put("db", db_tb_ary[0]);
							tempjson.put("tb", db_tb_ary[1]);
							tempjson.put("field", field);
							tempjson.put("index", i);
							tempjson.put("rule", filterPattern.name());
							tempjson.put("ruleValue", ruleAry[i]);
							ret.add(tempjson);
						}
					}
				}
			}
			retall.addAll(ret);
		}
		return retall;
	}

	// 把grid数据转为string放入输入框
	public static String toString(JSONArray ruleJsonAry) {
		TreeSet<FilterRulePo> tempset = new TreeSet<FilterPattern.FilterRulePo>();
		for (int i = 0; i < ruleJsonAry.size(); i++) {
			JSONObject jsonObject = ruleJsonAry.getJSONObject(i);
			FilterRulePo temppo = new FilterRulePo();
			temppo.setDb(jsonObject.getString("db"));
			temppo.setTb(jsonObject.getString("tb"));
			temppo.setField(jsonObject.getString("field"));
			temppo.setIndex(jsonObject.getIntValue("index"));
			temppo.setRule(FilterPattern.valueOf(jsonObject.getString("rule")));
			temppo.setRuleValue(jsonObject.getString("ruleValue"));
			tempset.add(temppo);
		}
		StringBuffer buff = new StringBuffer();

		for (FilterRulePo filterRulePo : tempset) {
			String tempstr = String.format(ruleFormat, filterRulePo.getDb(), filterRulePo.getTb(),
					filterRulePo.getField(), filterRulePo.getRule(), filterRulePo.getIndex(),
					filterRulePo.getRuleValue());
			buff.append("\n" + tempstr);
		}
		return buff.length() > 0 ? buff.substring(1) : "";
	}

	@Data
	private static class FilterRulePo implements Comparable<FilterRulePo> {
		private String db;
		private String tb;
		private String field;
		private int index;
		private FilterPattern rule;
		private String ruleValue;

		@Override
		public int compareTo(FilterRulePo o) {
			int dbDif = this.db.compareToIgnoreCase(o.getDb());
			if (dbDif != 0) {
				return dbDif;
			}
			int tbDif = this.tb.compareToIgnoreCase(o.getTb());
			if (tbDif != 0) {
				return tbDif;
			}
			int fieldDif = this.field.compareToIgnoreCase(o.getField());
			if (fieldDif != 0) {
				return fieldDif;
			}
			return this.index - o.getIndex();
		}
	}

	public String getDesc() {
		return desc;
	}

	private FilterPattern(String desc) {
		this.desc = desc;
	}

	public static Map<FilterPattern, Map<String, Map<String, Set<String>>>> packageFilterRules(String filterStr) {
		Map<FilterPattern, Map<String, Map<String, Set<String>>>> returmap = new HashMap<FilterPattern, Map<String, Map<String, Set<String>>>>();
		Properties props = IOUtil.StringToProperties(filterStr);
		Map<String, String> propmap = CollectionUtil.getPropsByKeypre(props, "duckula.busi.filter", true);
		for (String ele : propmap.keySet()) {
			String[] tempKeyAry = ele.split("\\.");// 库、表、字段、模式、唯一标识(可以避免重复被覆盖，可以不填这个标识)，
			if (tempKeyAry.length < 4) {
				log.error("规则解析错误，key最少4个元素");
				continue;
			}
			FilterPattern pattern = FilterPattern.valueOf(tempKeyAry[3]);
			String db_tb = String.format(db_tb_formart, tempKeyAry[0], tempKeyAry[1]);
			Map<String, Set<String>> tempMap = null;
			Map<String, Map<String, Set<String>>> db_tb_map = returmap.get(pattern);
			if (db_tb_map == null) {
				db_tb_map = new HashMap<String, Map<String, Set<String>>>();
				returmap.put(pattern, db_tb_map);
			}
			if (db_tb_map.get(db_tb) == null) {
				tempMap = new HashMap<String, Set<String>>();
				db_tb_map.put(db_tb, tempMap);
			} else {
				tempMap = db_tb_map.get(db_tb);
			}
			String fieldName = tempKeyAry[2];
			Set<String> ruleSet = null;
			if (tempMap.get(fieldName) == null) {
				ruleSet = new HashSet<String>();
				tempMap.put(fieldName, ruleSet);
			} else {
				ruleSet = tempMap.get(fieldName);
			}
			ruleSet.add(propmap.get(ele));
		}
		return returmap;
	}

}
