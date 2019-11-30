package net.wicp.tams.duckula.ops.pages;

import org.apache.tapestry5.annotations.Import;
import org.apache.tapestry5.annotations.SessionState;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.util.TextStreamResponse;
import org.slf4j.Logger;

import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.ops.beans.SessionBean;

@Import(stack = "easyuistack")
public class Login {
	@Inject
	private Logger logger;

	@Inject
	protected Request request;

	@SessionState
	private SessionBean sessionBean;

	public TextStreamResponse onLogin() {
		String userName = request.getParameter("userName");
		String pwd = request.getParameter("pwd");
		if (StringUtil.isNull(userName) || StringUtil.isNull(pwd)) {
			return TapestryAssist.getTextStreamResponse(Result.getError("请输入用户名和密码!"));
		}
		if (!"admin".equals(userName) || !"admin123".equals(pwd)) {			
			return TapestryAssist.getTextStreamResponse(Result.getError("用户名或密码错误!"));
		}
		sessionBean = new SessionBean();
		sessionBean.setIsLogin(YesOrNo.yes);
		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

}
