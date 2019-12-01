package net.wicp.tams.duckula.common.beans;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.StrPattern;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

@Data
@Slf4j
public class Consumer {
	private String id;
	private String taskOnlineId;
	private String topic;
	private int partitionNum;// 分区数，在ops需要查到
	private String groupId;// 使用哪个groupId，为空则用默认的groupId
	private Long startPosition;// 从哪个位置开始监听
	private String rules;
	private YesOrNo run = YesOrNo.no;// 是否运行此任务,默认为false不运行,仅配置好,不做运行处理.
	private SenderConsumerEnum senderConsumerEnum;
	private final List<Rule> ruleList = new ArrayList<>();
	
	private MiddlewareType middlewareType;//中间件类型
	private String middlewareInst;//中间件配置

	//
	private Integer busiNum;
	private String busiPlugin;
	
	private Integer batchNum=50;//每次拉取数量
	private Integer batchTimeout=1000;//拉取的超时时间
	

	public void setRules(String rules) {
		this.rules = rules;
		if (StringUtil.isNull(rules)) {
			return;
		}
		ruleList.clear();
		String[] ruleAry = rules.split("&");
		for (int i = 0; i < ruleAry.length; i++) {
			String[] ruleValues = ruleAry[i].split("`");
			if (ruleValues.length == 0 || ruleValues.length != 3) {
				throw new IllegalArgumentException("规则长度只能为３!");
			}
			Rule rule = new Rule();
			rule.setDbPattern(buildPatter(ruleValues[0]));
			rule.setTbPattern(buildPatter(ruleValues[1]));
			JSONObject json = JSON.parseObject(ruleValues[2]);
			for (String key : json.keySet()) {
				RuleItem tempItem = RuleItem.get(key);
				if (tempItem == null) {
					log.error("规则设置出错，请检查【{}】是否在net.wicp.tams.duckula.plugin.constant.RuleItem中定义!",key);
					throw new IllegalArgumentException("规则设置出错，请检查【"+key+"】是否在net.wicp.tams.duckula.plugin.constant.RuleItem中定义!");
				} else {
					rule.getItems().put(tempItem, json.getString(key));
				}
			}
			ruleList.add(rule);
		}
	}

	private String buildPatter(String patter) {
		if (patter.endsWith("_")) {
			return String.format("^%s[0-9]*$", patter);
		} else {
			return String.format("^%s$", patter);
		}
	}

	/***
	 * 找到表的匹配规则
	 * 
	 * @param db
	 * @param tb
	 * @return
	 */
	public Rule findRule(String db, String tb) {
		for (Rule rule : this.getRuleList()) {
			if (!"^*$".equals(rule.getDbPattern())) {
				boolean retdb = StrPattern.checkStrFormat(rule.getDbPattern(), db);
				if (!retdb) {
					continue;
				}
			}
			if (!"^*$".equals(rule.getTbPattern())) {
				boolean rettb = StrPattern.checkStrFormat(rule.getTbPattern(), tb);
				if (!rettb) {
					continue;
				}
			}
			return rule;
		}
		return null;
	}
}
