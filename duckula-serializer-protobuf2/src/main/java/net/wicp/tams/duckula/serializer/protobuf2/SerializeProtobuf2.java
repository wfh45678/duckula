package net.wicp.tams.duckula.serializer.protobuf2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.duckula.client.Protobuf2.ColumnType;
import net.wicp.tams.duckula.client.Protobuf2.DuckulaCol;
import net.wicp.tams.duckula.client.Protobuf2.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf2.OptType;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.serializer.ISerializer;
/***
 * 
 * @author 偏锋书生
 *
 * @version 创建时间：2017年5月27日 下午10:19:50
 */
@Slf4j
public class SerializeProtobuf2 implements ISerializer {

	@Override
	public List<SingleRecord> serialize(DuckulaPackage duckulaPackage, String splitKey) {
		DuckulaEvent.Builder build = DuckulaEvent.newBuilder();
		build.setDb(duckulaPackage.getEventTable().getDb());
		build.setTb(duckulaPackage.getEventTable().getTb());
		OptType optType = OptType.valueOf(duckulaPackage.getEventTable().getOptType().getValue());
		build.setOptType(optType);
		build.setGtid(duckulaPackage.getEventTable().getGtid());
		build.setColNum(duckulaPackage.getEventTable().getColsNum());
		// 列名 TODO 注意，如果表删除时event.getEventTable().getCols()将为空
		build.addAllCols(Arrays.asList(duckulaPackage.getEventTable().getCols()));
		for (int i = 0; i < duckulaPackage.getEventTable().getColsType().length; i++) {// 列类型
			build.addColsType(ColumnType.valueOf(duckulaPackage.getEventTable().getColsType()[i]));
		}
		build.setIsError(duckulaPackage.isError());// 是否错误数据
		List<SingleRecord> retlist = new ArrayList<>();

		String keyValue;
		splitKey = StringUtil.isNotNull(splitKey) ? duckulaPackage.getEventTable().getCols()[0] : splitKey;// 如果没有传splitKey表示使用第一列
		for (int i = 0; i < duckulaPackage.getRowsNum(); i++) {
			DuckulaEvent.Builder rowbuilder = build.clone();
			switch (optType) {
			case insert:
				keyValue = initAfter(rowbuilder, duckulaPackage, splitKey, i);
				break;
			case delete:
				keyValue = initBefore(rowbuilder, duckulaPackage, splitKey, i);
				break;
			case update:
				keyValue = initAfter(rowbuilder, duckulaPackage, splitKey, i);
				initBefore(rowbuilder, duckulaPackage, splitKey, i);
				break;
			default:
				keyValue = "";
				break;
			}
			SingleRecord tempobj = SingleRecord.builder().key(keyValue).data(rowbuilder.build().toByteArray())
					.optType(duckulaPackage.getEventTable().getOptType()).db(duckulaPackage.getEventTable().getDb())
					.tb(duckulaPackage.getEventTable().getTb()).build();
			retlist.add(tempobj);
		}
		return retlist;
	}

	private String initAfter(DuckulaEvent.Builder rowbuilder, DuckulaPackage event, String splitKey, int rowNo) {
		String[] tempary = event.getAfters()[rowNo];
		String[] cols = event.getEventTable().getCols();
		boolean isError = cols.length != event.getEventTable().getColsNum();
		if (isError) {
			log.error("colsname not match binlog col,db:{},table:[{}],binlogcolcount:{},colname:{}",
					event.getEventTable().getDb(), event.getEventTable().getTb(), event.getEventTable().getColsNum(),
					cols);
			if (event.getEventTable().getColsNum() > cols.length) {// 列名数小于列数，无法处理停掉进程.列名数大于列数（在末尾加字段方式）可以继续运行
				LoggerUtil.exit(JvmStatus.s15);
			}
		}
		String retSplitValue = "";
		for (int i = 0; i < event.getEventTable().getColsNum(); i++) {
			if (tempary[i] != null) {
				DuckulaCol tempobj = DuckulaCol.newBuilder().setColName(cols[i]).setColValue(cols[i]).build();
				rowbuilder.addAfter(tempobj);
				if (splitKey.equals(cols[i])) {
					retSplitValue = cols[i];
				}
			}
		}
		return retSplitValue;

	}

	private String initBefore(DuckulaEvent.Builder rowbuilder, DuckulaPackage event, String splitKey, int rowNo) {
		String[] tempary = event.getBefores()[rowNo];
		String[] cols = event.getEventTable().getCols();
		boolean isError = cols.length != event.getEventTable().getColsNum();
		if (isError) {
			log.error("colsname not match binlog col,db:{},table:[{}],binlogcolcount:{},colname:{}",
					event.getEventTable().getDb(), event.getEventTable().getTb(), event.getEventTable().getColsNum(),
					cols);
			if (event.getEventTable().getColsNum() > cols.length) {// 列名数小于列数，无法处理停掉进程.列名数大于列数（在末尾加字段方式）可以继续运行
				LoggerUtil.exit(JvmStatus.s15);
			}
		}
		String retSplitValue = "";
		for (int i = 0; i < event.getEventTable().getColsNum(); i++) {
			if (tempary[i] != null) {
				DuckulaCol tempobj = DuckulaCol.newBuilder().setColName(cols[i]).setColValue(cols[i]).build();
				rowbuilder.addBefore(tempobj);
				if (splitKey.equals(cols[i])) {
					retSplitValue = cols[i];
				}
			}
		}
		return retSplitValue;
	}

}
