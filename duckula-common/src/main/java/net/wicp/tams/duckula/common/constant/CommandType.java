package net.wicp.tams.duckula.common.constant;

import java.io.File;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.EPlatform;
import net.wicp.tams.common.constant.JvmStatus;

@Slf4j
public enum CommandType {
	task("在线监听任务", "run", "duckula-task.properties", "duckula-task.jar", "docker-run.sh", "t-%s", "nojob",ZkPath.tasks),

	dump("全量导入处理任务", "dump", "duckula-dump-elasticsearch.properties", "duckula-dump-elasticsearch.jar",
			"docker-dump.sh", "d-%s", "now",ZkPath.dumps),

	consumer("kafka监听任务", "consumer", "duckula-kafka-consumer.properties", "duckula-kafka-consumer.jar",
			"docker-consumer.sh", "c-%s", "nojob",ZkPath.consumers);

	private final String desc;
	private final String commonPath;
	private final String propNamme;// 属性文件名
	private final String jarName;// 需要运行的jar包名，可适用于 grep查进程
	private final String k8scmd;
	private final String k8sFormate;// 以什么样的id形式出现在k8s集群
	private final String k8sSchedule;// 默认的k8s调度模式
	private final ZkPath zkPath;

	public ZkPath getZkPath() {
		return zkPath;
	}

	public String getK8sSchedule() {
		return k8sSchedule;
	}

	public String getK8sFormate() {
		return k8sFormate;
	}

	public String getK8scmd() {
		return k8scmd;
	}

	public String getJarName() {
		return jarName;
	}

	public String getK8sId(String id) {
		String idstr = id.replace("_", "-");
		if (idstr.length() >= 63) {// TODO 具63再定
			return String.format(this.k8sFormate, idstr.substring(0, 63));
		} else {
			return String.format(this.k8sFormate, idstr);
		}
	}

	private CommandType(String desc, String commonPath, String propNamme, String jarName, String k8scmd,
			String k8sFormate, String k8sSchedule,ZkPath zkPath) {
		this.desc = desc;
		this.commonPath = commonPath;
		this.propNamme = propNamme;
		this.jarName = jarName;
		this.k8scmd = k8scmd;
		this.k8sFormate = k8sFormate;
		this.k8sSchedule = k8sSchedule;
		this.zkPath=zkPath;
	}

	public String getDesc() {
		return desc;
	}

	public void setCommonProps() {
		setZkProps();
		String duckulaHome = System.getenv("DUCKULA_DATA");
		Properties curProps = IOUtil.fileToProperties(new File(String.format("%s/conf/%s", duckulaHome, propNamme)));
		if (curProps.isEmpty()) {
			log.error("没有取得属性文件,请确认设置了DUCKULA_DATA环境变量");
			LoggerUtil.exit(JvmStatus.s15);
		}
		Conf.overProp(curProps);
		setUserProps();
	}

	/***
	 * 由于docker等需要通过环境变量和属性文件来配置，所以要加
	 */
	public static void setUserProps() {
		// 环境变量处理
		Properties evnProps = new Properties();
		// 2、 ip地址处理(用于分布式锁，与zk没什么关系),在使用纯docker启动时需要打server的锁IP
		String ipstr = System.getenv("ip");
		if(StringUtil.isNotNull(ipstr)) {//ops不需要设置此环境变量
			evnProps.put("common.apiext.os.ip", StringUtil.hasNull(ipstr, ""));
		}
		Conf.overProp(evnProps);
	}

	public static void setZkProps() {
		String duckulaHome = System.getenv("DUCKULA_DATA");
		Properties zkprops = IOUtil.fileToProperties(new File(String.format("%s/conf/zk.properties", duckulaHome)));
		if (zkprops.isEmpty()) {
			log.error("没有取得zk属性配置文件,请确认设置了DUCKULA_DATA环境变量");
			LoggerUtil.exit(JvmStatus.s15);
		}
		// 环境变量要大于属性配置
		String zk = System.getenv("zk");
		if (StringUtil.isNotNull(zk)) {
			zkprops.put("common.others.zookeeper.constr", zk);
		}
		if (StringUtil.isNull(zkprops.get("common.others.zookeeper.constr"))) {
			log.error("需要zk的连接地址，可以在环境变量'zk'设置，也可以在zk.properties设置common.others.zookeeper.constr");
			LoggerUtil.exit(JvmStatus.s15);
		}
		Conf.overProp(zkprops);
	}

	public String getBatchFile(EPlatform ePlatform) {
		if (StringUtil.isNull(ePlatform.getBatchSuffix())) {
			return "";
		}
		return IOUtil.mergeFolderAndFilePath("/bin", this.commonPath + ePlatform.getBatchSuffix());
	}

	/***
	 * 得到docker执行时的 cmd中命令
	 * 
	 * @param ePlatform
	 *            平台
	 * @return
	 */

	public String getDockerCmd(EPlatform ePlatform) {
		return "docker-" + this.commonPath + ePlatform.getBatchSuffix();
	}

	public String getPropNamme() {
		return propNamme;
	}
}
