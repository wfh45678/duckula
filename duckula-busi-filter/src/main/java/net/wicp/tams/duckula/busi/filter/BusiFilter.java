package net.wicp.tams.duckula.busi.filter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang3.ArrayUtils;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.NumberUtil;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.common.constant.StrPattern;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectException;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.busi.IBusi;

@Slf4j
public class BusiFilter implements IBusi {

	private String[][] filterRules;

	@SuppressWarnings({ "unchecked" })
	public BusiFilter() {
		Properties props = IOUtil.fileToProperties(new File(
				IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"), "/conf/plugin/duckula-busi-filter.properties")));
		Conf.overProp(props);
		Map<String, String> propmap = Conf.getPre("duckula.busi.filter.filtered", true);

		List<String> keys = (List<String>) CollectionUtils.select(propmap.keySet(), new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				String key = String.valueOf(object);
				return StrPattern.checkStrFormat("^key[0-9]*$", key);
			}
		});
		filterRules = new String[keys.size()][2];
		for (int i = 0; i < keys.size(); i++) {
			String key = keys.get(i);
			filterRules[i] = new String[] { propmap.get(key), propmap.get(key + ".pattern") };
		}
		log.info("---------------------初始化完成-----------------------");
	}

	@Override
	public void doWith(DuckulaPackage duckulaPackage, Rule rule) throws ProjectException {
		List<Integer> remove = new ArrayList<>();
		for (String[] filterRule : filterRules) {
			int indexOf = ArrayUtils.indexOf(duckulaPackage.getEventTable().getCols(), filterRule[0]);
			if (indexOf >= 0) {
				String[][] valuestrue = OptType.delete == duckulaPackage.getEventTable().getOptType()
						? duckulaPackage.getBefores()
						: duckulaPackage.getAfters();
				for (int i = 0; i < valuestrue.length; i++) {
					String[] values = valuestrue[i];
					boolean checkResult = StrPattern.checkStrFormat(filterRule[1], values[indexOf]);
					if (!checkResult) {
						remove.add(i);
					}
				}
			}
		}
		int[] array = NumberUtil.toArray(remove);
		if (array.length > 0) {
			if (duckulaPackage.getBefores() != null) {
				String[][] valuesTrue = ArrayUtils.removeAll(duckulaPackage.getBefores(), array);
				if (valuesTrue.length == 0) {
					throw new ProjectException(ExceptAll.duckula_nodata, "过滤后没有数据");
				}
				duckulaPackage.setBefores(valuesTrue);
			}
			if (duckulaPackage.getAfters() != null) {
				String[][] valuesTrue = ArrayUtils.removeAll(duckulaPackage.getAfters(), array);
				if (valuesTrue.length == 0) {
					throw new ProjectException(ExceptAll.duckula_nodata, "过滤后没有数据");
				}
				duckulaPackage.setAfters(valuesTrue);
			}
		}
	}
}
