package net.wicp.tams.duckula.ops.pages;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tapestry5.Asset;
import org.apache.tapestry5.annotations.InjectPage;
import org.apache.tapestry5.annotations.OnEvent;
import org.apache.tapestry5.annotations.Path;
import org.apache.tapestry5.annotations.SessionState;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.internal.util.CollectionFactory;

import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.component.assistbean.Menu;
import net.wicp.tams.component.constant.ResType;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.ops.beans.SessionBean;

/**
 * Start page of application duckula-ops.
 */
public class Index {

	@Inject
	@Path("context:/menu.properties")
	private Asset asset;

	@SessionState
	private SessionBean sessionBean;

	private boolean sessionBeanExists;

	@InjectPage
	private Login login;

	@OnEvent(value = "switchMenu")
	public List<Menu> switchMenu(String moudleId) throws IOException {
		Properties prop = new Properties();
		prop.load(asset.getResource().openStream());
		String[] menus = prop.getProperty("menu.all").split(",");
		List<Menu> retlist = CollectionFactory.newList();
		for (int i = 0; i < menus.length; i++) {
			if (!menus[i].startsWith(moudleId)) {
				continue;
			}
			String id = prop.getProperty(String.format("%s.id", menus[i]));
			TaskPattern curTaskPattern = TaskPattern.getCurTaskPattern();
			if (ArrayUtils.contains(curTaskPattern.getExcludeMenu(), id)) {
				continue;
			}

			String resName = prop.getProperty(String.format("%s.resName", menus[i]));
			String resType = prop.getProperty(String.format("%s.resType", menus[i]));
			String resValue = prop.getProperty(String.format("%s.resValue", menus[i]));
			if (StringUtil.isNotNull(id) && StringUtil.isNotNull(resName) && StringUtil.isNotNull(resType)
					&& StringUtil.isNotNull(resValue)) {
				Menu menu = Menu.builder().id(id).resName(resName).resType(ResType.get(resType)).resValue(resValue)
						.build();
				retlist.add(menu);
			}
		}
		return retlist;
	}

	public Object onActivate() {
		if (!sessionBeanExists || sessionBean == null || sessionBean.getIsLogin() == YesOrNo.no) {
			return login;
		} else {
			return null;
		}
	}
}
