package net.wicp.tams.duckula.common.beans;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;

import lombok.Data;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.SimpleTreeNode;

@Data
public class Mapping {
	private String id;
	private String index;// 索引
	private String type = "_doc";// 类型
	private int shardsNum;// 分片数
	private int replicas;// 复制因子
	private String content;// 内容
	// 有下面的就能生成content
	private String dbinst;
	private String db;
	private String tb;
	// private String primary;
	// 第一个从表
	private String db1;
	private String tb1;
	private String rela1;

	public List<SimpleTreeNode> buildRelaNodes() {
		if (StringUtil.isNull(rela1)) {
			return null;
		}
		List<SimpleTreeNode> retlist = new ArrayList<>();
		SimpleTreeNode node = new SimpleTreeNode(tb, null);
		String subrela = String.format("%s:%s", tb1, rela1);
		SimpleTreeNode node2 = new SimpleTreeNode(subrela, tb);
		retlist.add(node);
		retlist.add(node2);
		return retlist;
	}

	/***
	 * 得到索引的列名，无序的
	 * 
	 * @return
	 */
	public String[] findColNames() {
		JSONObject json = JSONObject.parseObject(content, Feature.AllowSingleQuotes);
		String[] retAry = new String[json.size()];
		int i = 0;
		for (String key : json.keySet()) {
			retAry[i] = key;
			i++;
		}
		return retAry;
	}

}
