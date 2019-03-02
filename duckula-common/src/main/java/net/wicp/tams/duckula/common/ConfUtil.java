package net.wicp.tams.duckula.common;

import java.io.File;
import java.util.Properties;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.duckula.common.constant.MiddlewareType;

public abstract class ConfUtil {
	public static int defaulJmxPort=2723;
	public static int defaultDebugPort=2113;
	
	public static Properties configMiddleware(MiddlewareType middlewareType, String middlewareInst) {
		String mergeFolderAndFilePath = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"),
				String.format("/conf/%s/%s-%s.properties", middlewareType, middlewareType, middlewareInst));
		Properties retProps = IOUtil.fileToProperties(new File(mergeFolderAndFilePath));
		return retProps;
	}
	
	public static String getDatadir(boolean isconfig) {
		String datadir =isconfig? Conf.get("duckula.ops.datadir"):System.getenv("DUCKULA_DATA");
		return datadir;
	}
	
	public static void printlnASCII() {
		String formatestr="-----------------------------------  %s  --------------------------------------------";		
		System.out.println(String.format(formatestr, "     _            _          _       ___   "));
		System.out.println(String.format(formatestr, "    | |          | |        | |     |__ \\  "));
		System.out.println(String.format(formatestr, "  __| |_   _  ___| | ___   _| | __ _   ) | "));
		System.out.println(String.format(formatestr, " / _` | | | |/ __| |/ / | | | |/ _`  | / /  "));
		System.out.println(String.format(formatestr, "| (_| | |_| | (__|   <| |_| | | (_| |/ /_  "));
		System.out.println(String.format(formatestr, " \\__,_|\\__,_|\\___|_|\\_\\\\__,_|_|\\__,_|____| "));
		System.out.println(String.format(formatestr, "                                           "));
	}
}
