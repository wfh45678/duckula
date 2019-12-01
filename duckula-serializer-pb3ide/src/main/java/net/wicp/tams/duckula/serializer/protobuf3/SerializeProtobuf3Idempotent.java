package net.wicp.tams.duckula.serializer.protobuf3;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEventIdempotent;
import net.wicp.tams.duckula.client.Protobuf3.IdempotentEle;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.serializer.ISerializer;

@Slf4j
public class SerializeProtobuf3Idempotent implements ISerializer {

	@Override
	public List<SingleRecord> serialize(DuckulaPackage duckulaPackage, String splitKey) {
		DuckulaEventIdempotent.Builder build = DuckulaEventIdempotent.newBuilder();
		build.setDb(duckulaPackage.getEventTable().getDb());
		build.setTb(duckulaPackage.getEventTable().getTb());
		OptType optType = OptType.forNumber(duckulaPackage.getEventTable().getOptType().getValue());
		build.setOptType(optType);
		build.setCommitTime(duckulaPackage.getEventTable().getCommitTime());

		String[][] orivalues = optType == OptType.delete ? duckulaPackage.getBefores() : duckulaPackage.getAfters();
		String[] cols = duckulaPackage.getEventTable().getCols();
		int[] colsType = duckulaPackage.getEventTable().getColsType();
		int keyIndex[] = new int[] { 0 };// 默认为第一个列为主键
		if (StringUtil.isNotNull(splitKey)) {
			String[] keyCols = splitKey.split(",");
			keyIndex = new int[keyCols.length];
			for (int i = 0; i < keyCols.length; i++) {
				keyIndex[i] = ArrayUtils.indexOf(cols, keyCols[i]);
				build.addKeyNames(cols[keyIndex[i]]);// 设置keyName
				build.addKeyTypesValue(colsType[keyIndex[i]]);// 添加类型
			}
		} else {
			build.addKeyNames(cols[0]);// 默认为第一个列为主键
			build.addKeyTypesValue(colsType[0]);
		}
		for (int i = 0; i < duckulaPackage.getRowsNum(); i++) {
			IdempotentEle.Builder rowbuilder = IdempotentEle.newBuilder();
			for (int j = 0; j < keyIndex.length; j++) {
				rowbuilder.addKeyValues(orivalues[i][keyIndex[j]]);
			}
			build.addValues(rowbuilder);
		}
		List<SingleRecord> retlist = new ArrayList<SingleRecord>();
		SingleRecord addele = SingleRecord.builder().db(duckulaPackage.getEventTable().getDb())
				.tb(duckulaPackage.getEventTable().getTb()).optType(duckulaPackage.getEventTable().getOptType())
				.data(build.build().toByteArray()).build();
		retlist.add(addele);
		return retlist;
	}

}
