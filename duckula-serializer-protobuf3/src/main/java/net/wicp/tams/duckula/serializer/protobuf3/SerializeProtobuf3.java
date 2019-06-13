package net.wicp.tams.duckula.serializer.protobuf3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.serializer.ISerializer;

@Slf4j
public class SerializeProtobuf3 implements ISerializer {

	@Override
	public List<SingleRecord> serialize(DuckulaPackage duckulaPackage, String splitKey) {
		DuckulaEvent.Builder build = DuckulaEvent.newBuilder();
		build.setDb(duckulaPackage.getEventTable().getDb());
		build.setTb(duckulaPackage.getEventTable().getTb());
		OptType optType = OptType.forNumber(duckulaPackage.getEventTable().getOptType().getValue());
		build.setOptType(optType);
		build.setGtid(duckulaPackage.getEventTable().getGtid());
		build.setColNum(duckulaPackage.getEventTable().getColsNum());
		// 列名 TODO 注意，如果表删除时event.getEventTable().getCols()将为空
		build.addAllCols(Arrays.asList(duckulaPackage.getEventTable().getCols()));
		//20190613输出commit时间
		build.setCommitTime(duckulaPackage.getEventTable().getCommitTime());
		for (int i = 0; i < duckulaPackage.getEventTable().getColsType().length; i++) {// 列类型
			build.addColsTypeValue(duckulaPackage.getEventTable().getColsType()[i]);
		}
		build.setIsError(duckulaPackage.isError());// 是否错误数据
		List<SingleRecord> retlist = new ArrayList<>();
		for (int i = 0; i < duckulaPackage.getRowsNum(); i++) {
			DuckulaEvent.Builder rowbuilder = build.clone();
			boolean isAfter = true;
			switch (optType) {
			case insert:
				initAfter(rowbuilder, duckulaPackage, i);
				break;
			case delete:
				initBefore(rowbuilder, duckulaPackage, i);
				isAfter = false;
				break;
			case update:
				initAfter(rowbuilder, duckulaPackage, i);
				initBefore(rowbuilder, duckulaPackage, i);
				break;
			default:
				break;
			}
			DuckulaEvent event = rowbuilder.build();
			splitKey = StringUtil.isNull(splitKey) ? duckulaPackage.getEventTable().getCols()[0] : splitKey;// 如果没有传splitKey表示使用第一列
			String keyValue;
			if (isAfter) {
				keyValue = event.getAfterMap().get(splitKey);
			} else {
				keyValue = event.getBeforeMap().get(splitKey);
			}
			SingleRecord tempobj = SingleRecord.builder().key(keyValue).data(rowbuilder.build().toByteArray())
					.optType(duckulaPackage.getEventTable().getOptType()).db(duckulaPackage.getEventTable().getDb())
					.tb(duckulaPackage.getEventTable().getTb()).build();
			retlist.add(tempobj);
		}
		return retlist;
	}

	private void initAfter(DuckulaEvent.Builder rowbuilder, DuckulaPackage event, int rowNo) {
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
		for (int i = 0; i < event.getEventTable().getColsNum(); i++) {
			if (tempary[i] != null) {
				rowbuilder.putAfter(cols[i], tempary[i]);
			}
		}
	}

	private void initBefore(DuckulaEvent.Builder rowbuilder, DuckulaPackage event, int rowNo) {
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
		for (int i = 0; i < event.getEventTable().getColsNum(); i++) {
			if (tempary[i] != null) {
				rowbuilder.putBefore(cols[i], tempary[i]);
			}
		}
	}

}
