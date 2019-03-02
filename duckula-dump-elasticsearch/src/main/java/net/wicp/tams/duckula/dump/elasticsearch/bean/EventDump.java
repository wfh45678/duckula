package net.wicp.tams.duckula.dump.elasticsearch.bean;

import lombok.Data;
import net.wicp.tams.common.es.EsData;

/****
 * dump请求的数据和控制信息
 * 
 * @author 偏锋书生
 *
 *         2018年4月26日
 */
@Data
public class EventDump {
	private String beginId;
	private String endId;
	private EsData.Builder esDataBuilder;
}
