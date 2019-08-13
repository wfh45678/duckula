package net.wicp.tams.duckula.busi.filter;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.common.constant.StrPattern;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectException;
import net.wicp.tams.common.jdbc.DruidAssit;
import net.wicp.tams.common.thread.ThreadPool;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.busi.IBusi;

@Slf4j
public class BusiFilter implements IBusi {

	// private String[][] filterRules;
	// key:库名|表名 value:key:字段名 value[0]模式 value[1]模式值
	private Map<String, Map<String, String[]>> filterRules = new HashMap<String, Map<String, String[]>>();
	private final String db_tb_formart = "%s|%s";

	public BusiFilter() {
		Properties props = IOUtil.fileToProperties(new File(IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"),
				"/conf/plugin/duckula-busi-filter.properties")));
		Conf.overProp(props);
		Map<String, String> propmap = Conf.getPre("duckula.busi.filter", true);
		for (String key : propmap.keySet()) {
			String[] tempKeyAry = key.split("\\.");// 库、表、字段、模式
			String db_tb = String.format(db_tb_formart, tempKeyAry[0], tempKeyAry[1]);
			Map<String, String[]> tempmap = filterRules.get(db_tb);
			if (tempmap == null) {
				tempmap = new HashMap<String, String[]>();
				filterRules.put(db_tb, tempmap);
			}
			Pattern pattern = Pattern.valueOf(tempKeyAry[3]);
			String value = propmap.get(key);
			switch (pattern) {
			case regular:
				break;
			case sql:
				break;
			default:
				break;
			}
			tempmap.put(tempKeyAry[2], new String[] { pattern.name(), value });

		}
		log.info("---------------------初始化完成-----------------------");
	}

	@Override
	public void doWith(DuckulaPackage duckulaPackage, Rule rule) throws ProjectException {
		// List<Integer> remove = new ArrayList<>();
		Map<Integer, Boolean> remove = new HashMap<Integer, Boolean>();
		// Map<Integer, Boolean> remove = new ConcurrentHashMap<Integer, Boolean>();
		Map<String, String[]> filters = filterRules.get(String.format(db_tb_formart,
				duckulaPackage.getEventTable().getDb(), duckulaPackage.getEventTable().getTb()));
		if (filters != null) {
			String[][] valuestrue = OptType.delete == duckulaPackage.getEventTable().getOptType()
					? duckulaPackage.getBefores()
					: duckulaPackage.getAfters();
			for (String col : filters.keySet()) {
				// remove.clear();如果有多个过滤条件，是叠加，不要clear
				int indexOf = "_".equals(col) ? -2 : ArrayUtils.indexOf(duckulaPackage.getEventTable().getCols(), col);//
				String[] vals = filters.get(col);
				Pattern pattern = Pattern.valueOf(vals[0]);
				String value = vals[1];
				final CountDownLatch latch = new CountDownLatch(valuestrue.length);
				for (int i = 0; i < valuestrue.length; i++) {
					filter(duckulaPackage, remove, valuestrue, indexOf, pattern, value, i);
					// log.info("filter 后:{},i:{}",remove.size(),i);
					final int index = i;
					ThreadPool.getDefaultPool().submit(new Runnable() {

						@Override
						public void run() {
							try {
								filter(duckulaPackage, remove, valuestrue, indexOf, pattern, value, index);
							} catch (Exception e) {
								log.error("过滤失败:" + duckulaPackage.getEventTable().getTb() + ":" + valuestrue[index][0],
										e);
							} finally {
								latch.countDown();
							}
						}
					});
				}
				try {
					latch.await(240, TimeUnit.SECONDS); // latch.await();
					// log.info("remove:" + remove.size());
				} catch (InterruptedException e) {
					log.error("等待CountDownLatch超时", e);
				}

			}
		}

		if (remove.size() > 0) {
			int[] array = new int[remove.size()]; // remove.keySet() .toArray(new Integer[remove.size()]);
			int tempindex = 0;
			for (Integer i : remove.keySet()) {
				array[tempindex++] = i.intValue();
			}
			boolean isnull = false;
			if (ArrayUtils.isNotEmpty(duckulaPackage.getBefores())) {
				int tempsize = duckulaPackage.getBefores().length;
				String[][] valuesTrue = ArrayUtils.removeAll(duckulaPackage.getBefores(), array);
				// log.info("before:{},remove:{},valuesTrue:{}", tempsize, array.length,
				// valuesTrue.length);
				duckulaPackage.setBefores(valuesTrue);
				if (valuesTrue.length == 0) {
					isnull = true;
					// throw new ProjectException(ExceptAll.duckula_nodata, "过滤后没有数据");
				}
			}
			if (ArrayUtils.isNotEmpty(duckulaPackage.getAfters())) {
				int tempsize = duckulaPackage.getAfters().length;
				String[][] valuesTrue = ArrayUtils.removeAll(duckulaPackage.getAfters(), array);
				// log.info("after:{},remove:{},valuesTrue:{}", tempsize, array.length,
				// valuesTrue.length);
				duckulaPackage.setAfters(valuesTrue);
				if (valuesTrue.length == 0) {
					isnull = true;
					// throw new ProjectException(ExceptAll.duckula_nodata, "过滤后没有数据");
				}
			}
			if (isnull) {
				throw new ProjectException(ExceptAll.duckula_nodata, "过滤后没有数据");
			}
		}
	}

	private void filter(DuckulaPackage duckulaPackage, Map<Integer, Boolean> remove, String[][] valuestrue, int indexOf,
			Pattern pattern, String value, int i) {
		String[] values = valuestrue[i];
		switch (pattern) {
		case regular:
			boolean checkResult = StrPattern.checkStrFormat(value, values[indexOf]);
			if (!checkResult) {
				remove.put(i, true);
			}
			break;
		case sql:
			String[] colNameFormSql = getColNameFormSql(value);
			String sql = value;
			for (String tempCol : colNameFormSql) {
				sql = sql.replace(String.format("${%s}", tempCol), "?");
			}
			String[] queryParams = new String[colNameFormSql.length];
			for (int j = 0; j < colNameFormSql.length; j++) {
				int indexOf2 = ArrayUtils.indexOf(duckulaPackage.getEventTable().getCols(), colNameFormSql[j]);
				if (StringUtil.isNull(values[indexOf2])) {// 20190813 如果有值为空就直接过滤
					remove.put(i, true);
					return;
				} else {
					queryParams[j] = values[indexOf2];
				}
			}
			Connection conn = null;
			PreparedStatement prst = null;
			try {
				conn = DruidAssit.getInst().getConnection();
				prst = conn.prepareStatement(sql);
				JdbcAssit.setPreParam(prst, queryParams);
				ResultSet rs = prst.executeQuery();
				if (!rs.next()) {
					remove.put(i, true);
					// log.info("remo:{}", i);
				} else {
					log.info("need send:{},remove:{}", i, remove.size());
				}
				rs.close();
			} catch (Exception e) {
				log.error("查询error", e);
			} finally {
				try {
					if (prst != null) {
						prst.close();
					}
					if (conn != null) {
						conn.close();
					}
				} catch (Exception e2) {
					log.error("close conn error", e2);
				}
			}
			break;
		default:
			break;
		}
	}

	private String[] getColNameFormSql(String sql) {
		List<String> retlist = new ArrayList<String>();
		int i = 0;
		while (true) {
			int j = sql.indexOf("${", i);
			if (j > 0) {
				int k = sql.indexOf("}", j);
				String temp = sql.substring(j + 2, k);
				i = k + 1;
				retlist.add(temp);
			} else {
				break;
			}
		}
		return retlist.toArray(new String[retlist.size()]);
	}
	/*
	 * public static void main(String[] args) { String[] colNameFormSql =
	 * getColNameFormSql("select 1 from athena.t_preinvoice where PRE_INVOICE_ID=${PRE_INVOICE_ID} and abc=${ddd} and SELLER_TENANT_CODE='Walmart' limit 1"
	 * ); System.out.println(colNameFormSql); }
	 */
}
