package net.wicp.tams.duckula.serializer.protobuf3.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import net.wicp.tams.duckula.client.DuckulaAssit;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;

public class TestKafka {
	static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Test
	public void testPackObj() throws FileNotFoundException, IOException {
		byte[] data = DuckulaAssit.getBytes("D:/temp/binlog/aaa");
		DuckulaEvent context = DuckulaAssit.parse(data);
		String name = DuckulaAssit.getValue(context, "name", false);
		System.out.println(name);

		Date createDate = DuckulaAssit.getValue(context, "create_date", false);
		System.out.println("channel_id=" +  format.format(createDate));

	}

}
