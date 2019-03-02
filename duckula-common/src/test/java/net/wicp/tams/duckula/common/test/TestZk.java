package net.wicp.tams.duckula.common.test;

import java.io.File;
import java.util.Properties;

import org.junit.Test;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.duckula.common.ZkClient;

@Slf4j
public class TestZk {

	private void init() {
		File propfile = new File(String.format("%s/conf/duckula-ops.properties", System.getenv("DUCKULA_DATA")));
		Properties props = IOUtil.fileToProperties(propfile);
		Conf.overProp(props);
	}

	@Test
	public void doDel() {
		init();
		Result deleteNode = ZkClient.getInst().deleteNode("/duckula/tasks/file_type_test");
		log.info("----"+deleteNode.isSuc());
	}
}
