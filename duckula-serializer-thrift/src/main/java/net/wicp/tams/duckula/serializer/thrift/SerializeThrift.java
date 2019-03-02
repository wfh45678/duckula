package net.wicp.tams.duckula.serializer.thrift;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.duckula.client.ColumnType;
import net.wicp.tams.duckula.client.DuckulaEvent;
import net.wicp.tams.duckula.client.OptType;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.serializer.ISerializer;

@Slf4j
public class SerializeThrift implements ISerializer {

	@Override
	public List<SingleRecord> serialize(DuckulaPackage duckulaPackage, String splitKey) {
		DuckulaEvent event = new DuckulaEvent();
		event.setDb(duckulaPackage.getEventTable().getDb());
		event.setTb(duckulaPackage.getEventTable().getTb());
		OptType optType = OptType.findByValue(duckulaPackage.getEventTable().getOptType().getValue());
		event.setOptType(optType);
		event.setGtid(duckulaPackage.getEventTable().getGtid());
		event.setCols(Arrays.asList(duckulaPackage.getEventTable().getCols()));
		event.setColNum(duckulaPackage.getEventTable().getColsNum());
		for (int i = 0; i < duckulaPackage.getEventTable().getColsType().length; i++) {// 列类型
			event.addToColsType(ColumnType.findByValue(duckulaPackage.getEventTable().getColsType()[i]));
		}
		// 列名 TODO 注意，如果表删除时event.getEventTable().getCols()将为空
		event.setCols(Arrays.asList(duckulaPackage.getEventTable().getCols()));
		event.setIsError(duckulaPackage.isError());// 是否错误数据

		List<SingleRecord> retlist = new ArrayList<>();
		for (int i = 0; i < duckulaPackage.getRowsNum(); i++) {
			DuckulaEvent rowEvent = null;
			try {
				rowEvent = (DuckulaEvent) BeanUtils.cloneBean(event);
			} catch (Exception e) {
				log.error("复制原型数据出错", e);
			}

			boolean isAfter = true;
			switch (optType) {
			case insert:
				initAfter(rowEvent, duckulaPackage, i);
				break;
			case deleted:
				initBefore(rowEvent, duckulaPackage, i);
				isAfter = false;
				break;
			case update:
				initAfter(rowEvent, duckulaPackage, i);
				initBefore(rowEvent, duckulaPackage, i);
				break;
			default:
				break;
			}
			splitKey = StringUtil.isNotNull(splitKey) ? duckulaPackage.getEventTable().getCols()[0] : splitKey;// 如果没有传splitKey表示使用第一列
			String keyValue;
			if (isAfter) {
				keyValue = event.getAfter().get(splitKey);
			} else {
				keyValue = event.getAfter().get(splitKey);
			}

			try {
				TMemoryBuffer mb0 = new TMemoryBuffer(32);
				TProtocol prot1 = new org.apache.thrift.protocol.TBinaryProtocol(mb0);
				rowEvent.write(prot1);
				SingleRecord tempobj = SingleRecord.builder().key(keyValue).data(prot1.readBinary().array())
						.optType(duckulaPackage.getEventTable().getOptType()).db(duckulaPackage.getEventTable().getDb())
						.tb(duckulaPackage.getEventTable().getTb()).build();
				retlist.add(tempobj);
			} catch (Exception e) {
				log.error("序列化出错", e);
				LoggerUtil.exit(JvmStatus.s15);
			}

		}
		return retlist;
	}

	private void initAfter(DuckulaEvent rowEvent, DuckulaPackage event, int rowNo) {
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
				rowEvent.putToAfter(cols[i], tempary[i]);
			}
		}
	}

	private void initBefore(DuckulaEvent rowEvent, DuckulaPackage event, int rowNo) {
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
				rowEvent.putToBefore(cols[i], tempary[i]);
			}
		}
	}

}
