package net.wicp.tams.duckula.dump.elasticsearch.handlerConsumer;

import java.util.Arrays;

import com.lmax.disruptor.WorkHandler;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Plugin;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.es.EsData;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.constant.PluginType;
import net.wicp.tams.duckula.dump.elasticsearch.bean.EventDump;
import net.wicp.tams.duckula.plugin.pluginAssit;
import net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer;

@Slf4j
public class BusiHander implements WorkHandler<EventDump> {

	private final IBusiConsumer<EsData.Builder> busiEs;

	@SuppressWarnings("unchecked")
	public BusiHander(Dump dump) {
		if (StringUtil.isNotNull(dump.getBusiPlugin())) {
			try {
				String plugDir = dump.getBusiPlugin().replace(".tar", "");
				String plugDirSys = IOUtil.mergeFolderAndFilePath(PluginType.consumer.getPluginDir(false), plugDir);
				ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
				Plugin plugin = pluginAssit.newPlugin(plugDirSys,
						"net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer", classLoader,
						"net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer",
						"net.wicp.tams.duckula.plugin.receiver.ReceiveAbs");
				Thread.currentThread().setContextClassLoader(plugin.getLoad().getClassLoader());// 需要加载前设置好classload
				this.busiEs = (IBusiConsumer<EsData.Builder>) plugin.newObject();
				log.info("the busi plugin is sucess");
			} catch (Throwable e) {
				log.error("组装业务插件失败", e);
				LoggerUtil.exit(JvmStatus.s9);
				throw new RuntimeException("组装业务插件失败");
			}
		} else {
			this.busiEs = null;
		}
	}

	@Override
	public void onEvent(final EventDump event) throws Exception {
		Thread.currentThread().setName("BusiHanderThread");
		if (busiEs == null) {
			return;
		}
		try {
			this.busiEs.doBusi(Arrays.asList(new EsData.Builder[] { event.getEsDataBuilder() }));
		} catch (Throwable e) {
			log.error("业务处理失败", e);// TODO
			LoggerUtil.exit(JvmStatus.s15);
		}
	}
}
