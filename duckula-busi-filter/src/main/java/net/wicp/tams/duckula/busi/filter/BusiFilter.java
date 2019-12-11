package net.wicp.tams.duckula.busi.filter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.OptType;
import net.wicp.tams.common.constant.StrPattern;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectException;
import net.wicp.tams.common.jdbc.DruidAssit;
import net.wicp.tams.common.thread.ThreadPool;
import net.wicp.tams.common.thread.threadlocal.PerthreadManager;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.FilterPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.busi.IBusi;

@Slf4j
public class BusiFilter implements IBusi {

	private Map<String, String[]> colNamesMap = new HashMap<String, String[]>();

	private final Map<FilterPattern, Map<String, Map<String, Set<String>>>> filterRulesMap;

	public BusiFilter() {
		String taskId = PerthreadManager.getInstance().createValue("duckula-taskId", String.class).get("");
		if (StringUtil.isNull(taskId)) {
			log.error("busiFilter不能获取taskId");
			LoggerUtil.exit(JvmStatus.s15);
		}
		// 添加默认配置，由于classload在下层，Conf初始化已在上层做掉了，没办法再次load下层的jar包中的默认配置
		Properties defaultProps = IOUtil.fileToProperties("/filter-default.properties", BusiFilter.class);
		Conf.overProp(defaultProps);

		Properties props = new Properties();
		// task的配置
		Task task = ZkUtil.buidlTask(taskId);
		props.put("common.jdbc.datasource.default.host", task.getIp());
		props.put("common.jdbc.datasource.default.port", task.getPort());
		props.put("common.jdbc.datasource.default.username", task.getUser());
		props.put("common.jdbc.datasource.default.password", task.getPwd());
		Conf.overProp(props);

		// zk上的配置
		String filterStr = ZkClient.getInst().getZkDataStr(ZkPath.filter.getPath(taskId));
		this.filterRulesMap = FilterPattern.packageFilterRules(filterStr);
		if (this.filterRulesMap == null) {// 没有过滤条件,或是转换失败
			LoggerUtil.exit(JvmStatus.s15);
		}
		// colname特殊处理
		if (MapUtils.isNotEmpty(this.filterRulesMap.get(FilterPattern.colname))) {
			Connection connection = DruidAssit.getConnection();
			for (String db_tb : this.filterRulesMap.get(FilterPattern.colname).keySet()) {
				String[] dbtbAry = db_tb.split("\\|");
				String[] primary = MySqlAssit.getPrimary(connection, dbtbAry[0], dbtbAry[1]);
				String value = this.filterRulesMap.get(FilterPattern.colname).get(db_tb).get("_").iterator().next();// 取第一条,因为只有一条
				String[] colNamesFilterAry = value.split(",");
				String[] arrayOr = CollectionUtil.arrayOr(String[].class, primary, colNamesFilterAry);
				colNamesMap.put(db_tb, arrayOr);
			}
			try {
				connection.close();
			} catch (SQLException e) {
				log.error("关闭connection失败", e);
			}
		}

		log.info("---------------------初始化完成-----------------------");
	}

	@Override
	public void doWith(DuckulaPackage duckulaPackage, Rule rule) throws ProjectException {
		// List<Integer> remove = new ArrayList<>();
		Map<Integer, Boolean> remove = new HashMap<Integer, Boolean>();
		// Map<Integer, Boolean> remove = new ConcurrentHashMap<Integer, Boolean>();
		String db_tb = String.format(FilterPattern.db_tb_formart, duckulaPackage.getEventTable().getDb(),
				duckulaPackage.getEventTable().getTb());

		for (FilterPattern filterPattern : filterRulesMap.keySet()) {
			if ("col".equals(filterPattern.getGroup())) {// 列过滤后面处理
				// 列过滤在后面做，且只会存在一个
				continue;
			}
			Map<String, Map<String, Set<String>>> dbtbMap = filterRulesMap.get(filterPattern);// 模式sql/col等
			if (MapUtils.isNotEmpty(dbtbMap) && MapUtils.isNotEmpty(dbtbMap.get(db_tb))) {// 需要过滤
				String[][] valuestrue = OptType.delete == duckulaPackage.getEventTable().getOptType()
						? duckulaPackage.getBefores()
						: duckulaPackage.getAfters();
				Map<String, Set<String>> fieldmap = dbtbMap.get(db_tb);// 列名->过滤条件
				for (String col : fieldmap.keySet()) {
					// remove.clear();如果有多个过滤条件，是叠加，不要clear
					int indexOf = "_".equals(col) ? -2
							: ArrayUtils.indexOf(duckulaPackage.getEventTable().getCols(), col);//
					for (String field : fieldmap.keySet()) {// 过滤条件的值
						String[] values = fieldmap.get(field).toArray(new String[fieldmap.get(field).size()]);// 过滤条件
						for (int i = 0; i < values.length; i++) {// 每个过滤条件都需要执行一次
							final int ruleIndex = i;
							final CountDownLatch latch = new CountDownLatch(valuestrue.length);
							for (int j = 0; j < valuestrue.length; j++) {// 每个数据都要过滤
								final int index = j;
								ThreadPool.getDefaultPool().submit(new Runnable() {

									@Override
									public void run() {
										try {
											filter(duckulaPackage, remove, valuestrue, indexOf, filterPattern,
													values[ruleIndex], index);
										} catch (Exception e) {
											log.error("过滤失败:" + duckulaPackage.getEventTable().getTb() + ":"
													+ valuestrue[index][0], e);
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
				}
			}
		}

		if (remove.size() > 0) {
			// remove.size() 不可靠，有时会少一个元素 remove.keySet().toArray(new
			// Integer[remove.size()]);
			Object[] array2 = remove.keySet().toArray();
			int[] array = new int[array2.length];
			for (int i = 0; i < array2.length; i++) {
				array[i] = (Integer) array2[i];
			}

			boolean isnull = false;
			int rowsNumRrue = 0;
			if (ArrayUtils.isNotEmpty(duckulaPackage.getBefores())) {
				String[][] valuesTrue = ArrayUtils.removeAll(duckulaPackage.getBefores(), array);
				// log.info("before:{},remove:{},valuesTrue:{}", tempsize, array.length,
				// valuesTrue.length);
				duckulaPackage.setBefores(valuesTrue);
				rowsNumRrue = valuesTrue == null ? 0 : valuesTrue.length;
				// duckulaPackage.setRowsNum(valuesTrue.length);
				if (valuesTrue.length == 0) {
					isnull = true;
					// throw new ProjectException(ExceptAll.duckula_nodata, "过滤后没有数据");
				}
			}
			if (ArrayUtils.isNotEmpty(duckulaPackage.getAfters())) {
				String[][] valuesTrue = ArrayUtils.removeAll(duckulaPackage.getAfters(), array);
				// log.info("after:{},remove:{},valuesTrue:{}", tempsize, array.length,
				// valuesTrue.length);
				duckulaPackage.setAfters(valuesTrue);
				// 取最小值
				rowsNumRrue = valuesTrue == null ? 0
						: ((rowsNumRrue == 0 || (rowsNumRrue > 0 && rowsNumRrue > valuesTrue.length))
								? valuesTrue.length
								: rowsNumRrue);
				// duckulaPackage.setRowsNum(valuesTrue.length);
				if (valuesTrue.length == 0) {
					isnull = true;
					// throw new ProjectException(ExceptAll.duckula_nodata, "过滤后没有数据");
				}
			}
			duckulaPackage.setRowsNum(rowsNumRrue);
			if (isnull) {
				throw new ProjectException(ExceptAll.duckula_nodata, "过滤后没有数据");
			}
		}

		String[] colFilter = colNamesMap.get(db_tb);
		if (ArrayUtils.isNotEmpty(colFilter)) {// 处理列过滤
			List<Integer> removeIndex = new ArrayList<Integer>();
			for (int i = 0; i < duckulaPackage.getEventTable().getCols().length; i++) {
				if (!ArrayUtils.contains(colFilter, duckulaPackage.getEventTable().getCols()[i])) {
					removeIndex.add(i);
				}
			}
			int[] removeIndexAry = new int[removeIndex.size()];
			for (int i = 0; i < removeIndex.size(); i++) {
				removeIndexAry[i] = removeIndex.get(i).intValue();
			}

			String[] cols = ArrayUtils.removeAll(duckulaPackage.getEventTable().getCols(), removeIndexAry);
			int[] colTypes = ArrayUtils.removeAll(duckulaPackage.getEventTable().getColsType(), removeIndexAry);
			if (ArrayUtils.isNotEmpty(duckulaPackage.getBefores())) {
				String[][] beforeTrue = new String[duckulaPackage.getBefores().length][];
				for (int i = 0; i < duckulaPackage.getBefores().length; i++) {
					beforeTrue[i] = ArrayUtils.removeAll(duckulaPackage.getBefores()[i], removeIndexAry);
				}
				duckulaPackage.setBefores(beforeTrue);
			}

			if (ArrayUtils.isNotEmpty(duckulaPackage.getAfters())) {
				String[][] afterTrue = new String[duckulaPackage.getAfters().length][];
				for (int i = 0; i < duckulaPackage.getAfters().length; i++) {
					afterTrue[i] = ArrayUtils.removeAll(duckulaPackage.getAfters()[i], removeIndexAry);
				}
				duckulaPackage.setAfters(afterTrue);
			}
			duckulaPackage.getEventTable().setCols(cols);
			duckulaPackage.getEventTable().setColsType(colTypes);
			duckulaPackage.getEventTable().setColsNum(cols.length);
		}
	}

	/***
	 * 
	 * @param duckulaPackage
	 * @param remove           需要删除id
	 * @param valuestrue       值
	 * @param filterColIndexOf 过滤列所在序号
	 * @param pattern          过滤模式
	 * @param filterValue      过滤条件的值
	 * @param rowIndex         第几行
	 */
	private void filter(DuckulaPackage duckulaPackage, Map<Integer, Boolean> remove, String[][] valuestrue,
			int filterColIndexOf, FilterPattern pattern, String filterValue, int rowIndex) {
		String[] values = valuestrue[rowIndex];// 数据的值
		switch (pattern) {
		case regular:
			boolean checkResult = StrPattern.checkStrFormat(filterValue, values[filterColIndexOf]);
			if (!checkResult) {
				remove.put(rowIndex, true);
			}
			break;
		case sql:
			String[] colNameFormSql = getColNameFormSql(filterValue);
			String sql = filterValue;
			for (String tempCol : colNameFormSql) {
				sql = sql.replace(String.format("${%s}", tempCol), "?");
			}
			String[] queryParams = new String[colNameFormSql.length];
			for (int j = 0; j < colNameFormSql.length; j++) {
				int indexOf2 = ArrayUtils.indexOf(duckulaPackage.getEventTable().getCols(), colNameFormSql[j]);
				if (StringUtil.isNull(values[indexOf2])) {// 20190813 如果有值为空就直接过滤
					remove.put(rowIndex, true);
					return;
				} else {
					queryParams[j] = values[indexOf2];
				}
			}
			Connection conn = null;
			PreparedStatement prst = null;
			try {
				conn = DruidAssit.getConnection();
				prst = conn.prepareStatement(sql);
				JdbcAssit.setPreParam(prst, queryParams);
				ResultSet rs = prst.executeQuery();
				if (!rs.next()) {
					remove.put(rowIndex, true);
					// log.info("remo:{}", i);
				} else {
					log.info("need send no:{},remove:{}", rowIndex, remove.size());
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
		case colname:// 不用处理，这个是需要在第二步处理
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
