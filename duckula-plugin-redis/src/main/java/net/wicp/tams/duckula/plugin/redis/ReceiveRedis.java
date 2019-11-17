package net.wicp.tams.duckula.plugin.redis;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import com.alibaba.fastjson.JSONObject;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.common.redis.RedisAssit;
import net.wicp.tams.common.redis.cachecloud.CacheCloudAssit;
import net.wicp.tams.common.redis.pool.AbsPool;
import net.wicp.tams.duckula.plugin.pluginAssit;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.receiver.ReceiveAbs;
import redis.clients.jedis.Jedis;

public class ReceiveRedis extends ReceiveAbs {

	private final Map<String, AbsPool> poolmap = new HashMap<>();

	public ReceiveRedis(JSONObject paramObjs) {
		super(paramObjs);
		Conf.overProp(props);
	}

	@Override
	public boolean receiveMsg(DuckulaPackage duckulaPackage, Rule rule, String splitKey) {
		AbsPool absPool = initPool(rule);
		Jedis jedis = absPool.getResource();
		OptType optType = duckulaPackage.getEventTable().getOptType();
		for (int i = 0; i < duckulaPackage.getRowsNum(); i++) {
			Map<String, String> data = pluginAssit.getUseData(duckulaPackage, i);
			CollectionUtil.filterNull(data, 1);// 过滤空值
			String keyValue = String.format(rule.getItems().get(RuleItem.key), data.get(splitKey));
			if (optType == OptType.delete) {
				jedis.del(keyValue);
			} else {
				jedis.hmset(keyValue, data);
				// jedis.set(keyValue, JSONObject.toJSONString(data));//
			}
		}
		AbsPool.returnResource(jedis);
		return true;
	}

	private volatile AbsPool absPoolMiddleConf;// 只有在中间件配置起作用时才会用。

	private AbsPool initPool(Rule rule) {
		String key = String.format("%s-%s", rule.getDbPattern(), rule.getTbPattern());
		if (!poolmap.containsKey(key)) {
			String appid = rule.getItems().get(RuleItem.appid);
			String redisurl = rule.getItems().get(RuleItem.redisurl);
			AbsPool absPool = null;
			if (StringUtil.isNotNull(appid)) {// cachecloud
				String[] splitAppIdAry = RuleItem.splitAppId(appid);
				if ("standalone".equals(splitAppIdAry[0])) {
					absPool = CacheCloudAssit.standalone(Long.parseLong(splitAppIdAry[1]));
				} else if ("sentinel".equals(splitAppIdAry[0])) {
					absPool = CacheCloudAssit.sentinel(Long.parseLong(splitAppIdAry[1]));
				} // TODO 集群模式
			} else if (StringUtil.isNull(redisurl)) {// redisurl为空,取中间件的配置
				if (absPoolMiddleConf == null) {
					synchronized (ReceiveRedis.class) {
						if (absPoolMiddleConf == null) {
							absPoolMiddleConf = RedisAssit.getPool();// 兼容standalone和sentinel
						}
					}
				}
				absPool = absPoolMiddleConf;
			} else {
				String redisurlValue = rule.getItems().get(RuleItem.redisurl);
				String[] hostAryl = RuleItem.splitRedisurl(redisurlValue);
				if ("standalone".equals(hostAryl[0])) {
					String[] split = hostAryl[1].split(":");
					absPool = RedisAssit.standalone(split[0], Integer.parseInt(split[1]), null);
				} else if ("sentinel".equals(hostAryl[0])) {
					absPool = RedisAssit.sentinel(hostAryl[1], ArrayUtils.subarray(hostAryl, 2, hostAryl.length));
				}
			}
			poolmap.put(key, absPool);
		}
		return poolmap.get(key);
	}

	@Override
	public boolean receiveMsg(List<SingleRecord> data, Rule rule) {
		AbsPool absPool = initPool(rule);
		Jedis jedis = absPool.getResource();
		for (SingleRecord singleRecord : data) {
			if (singleRecord.getOptType() == OptType.delete) {
				jedis.del(singleRecord.getKey());
			} else {
				try {
					jedis.set(singleRecord.getKey().getBytes("UTF-8"), singleRecord.getData());
				} catch (UnsupportedEncodingException e) {
				}
			}
		}
		AbsPool.returnResource(jedis);
		return true;
	}

	@Override
	public boolean isSync() {
		return true;
	}
}
