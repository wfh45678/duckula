package net.wicp.tams.duckula.dump.elasticsearch.handlerConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.lmax.disruptor.RingBuffer;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.jdbc.DruidAssit;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.dump.elasticsearch.bean.EventDump;

@Slf4j
public class Publisher implements Runnable {
	private final RingBuffer<EventDump> ringBuffer;

	private Connection connection;
	private PreparedStatement stmt;
	public static int numDuan;// 一段数量 默认500
	private final int numBatch = Integer.parseInt(Conf.get("duckula.dump.batch.num"));// 一次大批量数,默认20000
	private final long maxDuanNo;
	private final long maxBatchNo;
	private final String temp;
	private boolean isover = false;
	private final String startId;
	private final long numLastBatch;

	public Publisher(RingBuffer<EventDump> ringBuffer, Dump dump, String startId, Long recordNum) {
		this.ringBuffer = ringBuffer;
		numDuan = dump.getNumDuan() == null ? 500 : dump.getNumDuan();
		if (recordNum != null && recordNum >= 0) {
			maxBatchNo = recordNum / numBatch + (recordNum % numBatch == 0 ? 0 : 1);
			long lastBatchRecords = recordNum - (maxBatchNo - 1) * numBatch;
			maxDuanNo = (maxBatchNo - 1) * (numBatch / numDuan + (numBatch % numDuan == 0 ? 0 : 1))
					+ lastBatchRecords / numDuan + (lastBatchRecords % numDuan == 0 ? 0 : 1);
			numLastBatch = recordNum - (maxBatchNo - 1) * numBatch;
		} else {
			numLastBatch = numBatch;
			maxDuanNo = Long.MAX_VALUE;
			maxBatchNo = Long.MAX_VALUE;
		}
		this.startId = startId;
		String primaryName = dump.getPrimarys()[0];
		this.temp = String.format("select %s %s and %s>=?  order by %s limit ?,?", primaryName, dump.packFromstr(),
				primaryName, primaryName);
		log.info("--------maxDuanNo={},maxBatchNo={}-------", maxDuanNo == Long.MAX_VALUE ? -1 : maxDuanNo,
				maxBatchNo == Long.MAX_VALUE ? -1 : maxBatchNo);
	}

	private int batchNo = 0;
	private int duanNo = 0;
	private long timeBegin = 0;

	@Override
	public void run() {
		Thread.currentThread().setName("PublisherThread");
		String lastId = StringUtil.hasNull(this.startId, "");
		boolean isFirstBatch = true;
		boolean isLastBatch = false;
		this.timeBegin = System.currentTimeMillis();
		while (true) {
			try {
				if (this.connection == null || connection.isClosed()) {
					// 先关闭旧的stmt;
					if (this.stmt != null && this.stmt.isClosed()) {
						this.stmt.close();
					}
					this.connection = DruidAssit.getConnection();
					this.stmt = connection.prepareStatement(this.temp);
					this.stmt.setFetchSize(numBatch);
				}
			} catch (Exception e) {
				log.error("数据库连接不上", e);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
				}
				continue;
			}
			try {
				JdbcAssit.setPreParam(stmt, lastId, isFirstBatch ? 0 : 1,
						(isLastBatch && numLastBatch > 0) ? numLastBatch : numBatch);
				ResultSet rsDuan = stmt.executeQuery();
				int i = 0;
				String start = null;
				String end = null;
				if (!rsDuan.next()) {
					break;
				} else {
					start = rsDuan.getString(1);
					end = rsDuan.getString(1);
					i++;
				}
				while (rsDuan.next() && duanNo < maxDuanNo) {
					if (i == 0) {
						start = rsDuan.getString(1);
						end = rsDuan.getString(1);
						i++;
					} else if (i == numDuan - 1) {
						end = rsDuan.getString(1);
						pushData(start, end);
						lastId = end;
						duanNo++;
						i = 0;
						start = null;
						end = null;
					} else {
						end = rsDuan.getString(1);
						i++;
					}
				}
				// 最后一批数据
				if (start != null && end != null && duanNo < maxDuanNo) {
					pushData(start, end);
					duanNo++;
					lastId = end;
				}
				batchNo++;
				rsDuan.close();
			} catch (Exception e) {
				log.error("生产者执行失败", e);
				LoggerUtil.exit(JvmStatus.s15);
			}
			if (duanNo >= maxDuanNo) {// 需要有=号，否则会多做一次查询
				break;
			}
			isLastBatch = (batchNo == maxBatchNo - 1) ? true : false;
			isFirstBatch = false;
		}
		isover = true;
		try {
			stmt.close();
			connection.close();
		} catch (Exception e) {
			log.error("回收资源失败", e);
		}

	}

	private void pushData(String start, String end) {
		long sequence = ringBuffer.next();
		EventDump event = ringBuffer.get(sequence);
		event.setBeginId(start);
		event.setEndId(end);
		// System.out.println(start + " " + end);
		ringBuffer.publish(sequence);
		// System.out.println("-------min=" + ringBuffer.getMinimumGatingSequence() + "
		// size="
		// + (ringBuffer.getCursor() - ringBuffer.getMinimumGatingSequence()) + "
		// Cursor="
		// + ringBuffer.getCursor() + " batchNo=" + batchNo);
	}

	public int getBatchNo() {
		return batchNo;
	}

	public int getDuanNo() {
		return duanNo;
	}

	public boolean isIsover() {
		return isover;
	}

	public long getTimeBegin() {
		return timeBegin;
	}
}
