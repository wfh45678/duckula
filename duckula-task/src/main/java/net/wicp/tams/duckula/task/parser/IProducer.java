package net.wicp.tams.duckula.task.parser;

import net.wicp.tams.duckula.task.bean.EventPackage;

public interface IProducer {
	public void sendMsg(EventPackage sendBean);

	public EventPackage getNextBuild();

	public void stop();
}
