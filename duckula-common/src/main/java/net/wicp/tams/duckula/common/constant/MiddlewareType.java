package net.wicp.tams.duckula.common.constant;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.MapUtils;

import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.json.JSONUtil;
import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;

public enum MiddlewareType implements IEnumCombobox {
	kafka("kafka",true),

	es("elasticsearch",false),

	redis("redis",true);

	private final String desc;
	
	private final boolean isMiddleSave;

	public boolean isMiddleSave() {
		return isMiddleSave;
	}

	private Map<String, Map<String, String[]>> hostMap = new HashMap<>();// key:dev,value:mao<ip,hosts>.
																			// 需要设置的hosts，在docker和k8s中需要被配置

	private final List<String> eleList = new ArrayList<>();

	private volatile boolean isInit;// 为了热加载

	public void setInit(boolean isInit) {
		this.isInit = isInit;
	}

	/***
	 * 组装为/etc/hosts格式的string
	 * 
	 * @return
	 */
	public String getHostStr() {
		getEleList();
		if (MapUtils.isEmpty(hostMap)) {
			return "";
		}
		StringBuffer buff = new StringBuffer();
		for (String env : hostMap.keySet()) {
			Map<String, String[]> map = hostMap.get(env);
			for (String ip : map.keySet()) {
				String[] names = map.get(ip);
				for (String name : names) {
					buff.append(String.format("\n%s     %s", ip,name));
				}
			}
		}
		return buff.toString();
	}

	/**
	 * 得到中间件的所有配置
	 * 
	 * @return
	 */
	public List<String> getEleList() {
		if (!isInit) {
			synchronized (MiddlewareType.class) {
				if (!isInit) {
					String confpath = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/conf",
							this.name());
					File[] listFile = IOUtil.listFile(confpath, String.format("%s-\\w+.properties", this.name()));
					for (File file : listFile) {
						String ele = file.getName().replace(this.name() + "-", "").replace(".properties", "");
						Properties fileToProperties = IOUtil.fileToProperties(file);
						Map<String, String> propsByKeypre = CollectionUtil.getPropsByKeypre(fileToProperties,
								"docker.host.ip", true);
						Map<String, String[]> tempmap = new HashMap<>();
						for (String ip : propsByKeypre.keySet()) {
							tempmap.put(ip, propsByKeypre.get(ip).split(","));
						}
						this.hostMap.put(ele, tempmap);
						this.eleList.add(ele);
					}
					isInit = true;
				}
			}
		}
		return this.eleList;
	}

	public String getEleJson() {
		String retlist = JSONUtil.getJsonForListSimple(this.getEleList());
		return retlist;
	}

	private MiddlewareType(String desc,boolean isMiddleSave) {
		this.desc = desc;
		this.isMiddleSave=isMiddleSave;
	}

	public static MiddlewareType get(String name) {
		for (MiddlewareType pluginType : MiddlewareType.values()) {
			if (pluginType.name().equalsIgnoreCase(name)) {
				return pluginType;
			}
		}
		return null;
	}

	public static String getHosts() {
		StringBuffer buff = new StringBuffer();
		for (MiddlewareType middlewareType : MiddlewareType.values()) {
			buff.append(middlewareType.getHostStr());
		}
		return buff.toString();
	}

	public String getDesc() {
		return desc;
	}

	public Map<String, String[]> getHostMap(String inst) {
		if (!getEleList().contains(inst)) {
			return new HashMap<>();
		}
		return this.hostMap.get(inst);
	}

	@Override
	public String getName() {
		return this.name();
	}

	@Override
	public String getDesc_en() {
		return this.desc;
	}

	@Override
	public String getDesc_zh() {
		return this.desc;
	}
}
