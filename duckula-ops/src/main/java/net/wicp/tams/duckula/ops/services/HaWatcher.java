package net.wicp.tams.duckula.ops.services;

import java.io.UnsupportedEncodingException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.ops.servicesBusi.DuckulaAssitImpl;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

@Slf4j
public class HaWatcher implements PathChildrenCacheListener {
	private final CommandType commandType;

	public HaWatcher(CommandType commandType) {
		this.commandType = commandType;
	}

	private IDuckulaAssit duckulaAssit = new DuckulaAssitImpl();// TODO

	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		ChildData data = event.getData();
		switch (event.getType()) {
		case CHILD_ADDED:// 新增不关注
			System.out.println("Children_ADD : " + data.getPath() + "  数据:" + data.getData());
			break;
		case CHILD_REMOVED:
			ChangeChildren(data);
			break;
		case CHILD_UPDATED:// 修改不关注
			System.out.println("Children_UPDATE : " + data.getPath() + "  数据:" + data.getData());
			break;
		default:
			break;
		}
	}

	private void ChangeChildren(ChildData data) throws UnsupportedEncodingException {
		String[] paths = data.getPath().split("/");
		String childrenId = paths[paths.length - 2];
		duckulaAssit.reStartTask(commandType,childrenId,new String(data.getData(),"UTF-8"));
	}


}
