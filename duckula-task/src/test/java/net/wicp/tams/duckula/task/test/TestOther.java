package net.wicp.tams.duckula.task.test;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

import org.junit.Test;

import com.codahale.metrics.Clock;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;

public class TestOther {
	@Test
	public void testConf() throws IOException {
		Enumeration<URL> dirs = Thread.currentThread().getContextClassLoader()
				.getResources("net/wicp/tams");
		while (dirs.hasMoreElements()) {
			URL url = dirs.nextElement();
			System.out.println(url);
		}
		
		System.out.println(IOUtil.getCurFolder(Clock.class));
	}
	@Test
	public void testRoot() throws IOException {
		String str=   Conf.get("aaa");
		System.out.println(str);
	}
	
	@Test
	public void testASCII() {
		String formatestr="-----------------------------------  %s  --------------------------------------------";		
		System.out.println(String.format(formatestr, "                                     "));
		System.out.println(String.format(formatestr, "    | |          | |        | |      "));
		System.out.println(String.format(formatestr, "  __| |_   _  ___| | ___   _| | __ _ "));
		System.out.println(String.format(formatestr, "  / _` | | | |/ __| |/ / | | | |/ _` |"));
		System.out.println(String.format(formatestr, "| (_| | |_| | (__|   <| |_| | | (_| |"));
		System.out.println(String.format(formatestr, " \\__,_|\\__,_|\\___|_|\\_\\\\__,_|_|\\__,_|"));
		System.out.println(String.format(formatestr, "                                     "));
	}
	
}
