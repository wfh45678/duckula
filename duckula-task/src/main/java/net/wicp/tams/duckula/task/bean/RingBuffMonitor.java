package net.wicp.tams.duckula.task.bean;

import lombok.Data;

@Data
public class RingBuffMonitor {
  private long undoSize;//未处理单元
  private long senderUnit;//已发送单元
}
