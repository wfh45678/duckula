package net.wicp.tams.duckula.dump.elasticsearch.handlerConsumer;

import org.elasticsearch.action.bulk.BulkItemResponse;

import com.lmax.disruptor.WorkHandler;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.es.EsData;
import net.wicp.tams.common.es.bean.SettingsBean;
import net.wicp.tams.common.es.client.ESClient;
import net.wicp.tams.common.es.client.singleton.ESClientOnlyOne;
import net.wicp.tams.common.es.client.threadlocal.EsClientThreadlocal;
import net.wicp.tams.duckula.common.beans.Mapping;
import net.wicp.tams.duckula.dump.elasticsearch.MainDump;
import net.wicp.tams.duckula.dump.elasticsearch.bean.EventDump;

@Slf4j
public class SendHander implements WorkHandler<EventDump> {

	private final Mapping mapping;
	private final YesOrNo needsend;

	public SendHander(Mapping mapping, YesOrNo needsend) {
		this.mapping = mapping;
		this.needsend = needsend;
	}

	@Override
	public void onEvent(EventDump event) throws Exception {
		Thread.currentThread().setName("SendHanderThread");
		if (needsend != null && needsend == YesOrNo.no) {// 不需要发送
			log.info("不需要发送ES，发送ES的任务由插件完成");
			MainDump.metric.counter_send_es.inc(event.getEsDataBuilder().getDatasList().size());
			MainDump.metric.counter_send_event.inc();
			isOver();
			return;
		}

		ESClient eSClient = EsClientThreadlocal.createPerThreadEsClient();// 多实例没有改变性能
		// ESClient eSClient = ESClientOnlyOne.getInst().getESClient();
		// long curTime = System.currentTimeMillis();
		try {
			EsData esData = event.getEsDataBuilder().build();
			Result ret = null;
			if (esData.getDatasList().size() == 0) {// size为0不需要发送
				log.error("没有可发送的数据:", event);
				ret = Result.getSuc();
			} else {
				ret = eSClient.docWriteBatch_tc(esData);
			}
			if (!ret.isSuc()) {
				BulkItemResponse[] retObjs = (BulkItemResponse[]) ret.retObjs();
				for (BulkItemResponse bulkItemResponse : retObjs) {
					if (bulkItemResponse.isFailed()) {
						MainDump.metric.counter_send_error.inc();
						log.error(bulkItemResponse.getId());// TODO 错误处理
					}
				}
				LoggerUtil.exit(JvmStatus.s15);
			}
			MainDump.metric.counter_send_es.inc(esData.getDatasList().size());
			MainDump.metric.counter_send_event.inc();
		} catch (Exception e) {
			log.error("出异常了，需要处理", e);
			LoggerUtil.exit(JvmStatus.s15);
		} catch (Throwable e) {
			log.error("未捕获的异常", e);
			LoggerUtil.exit(JvmStatus.s15);
		} finally {
			// log.info("sendTime:{},num:{}", (System.currentTimeMillis() -
			// curTime),event.getEsDataBuilder().getDatasList().size());
			isOver();
		}
	}

	private void isOver() {
		if (MainDump.publisher.isIsover()
				&& MainDump.metric.counter_send_event.getCount() == MainDump.publisher.getDuanNo()) {
			ESClientOnlyOne.getInst().getESClient().indexSetting(mapping.getIndex(),
					SettingsBean.builder().refresh_interval(SettingsBean.fresh_null).build());
			LoggerUtil.exit(JvmStatus.s15);
		}
	}
}
