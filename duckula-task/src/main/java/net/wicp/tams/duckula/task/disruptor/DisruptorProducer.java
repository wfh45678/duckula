package net.wicp.tams.duckula.task.disruptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.FatalExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkerPool;
import com.lmax.disruptor.util.DaemonThreadFactory;

import net.wicp.tams.duckula.plugin.receiver.ReceiveAbs;
import net.wicp.tams.duckula.task.Main;
import net.wicp.tams.duckula.task.bean.EventPackage;
import net.wicp.tams.duckula.task.parser.IProducer;

public class DisruptorProducer implements IProducer {
	// 512 eden 260M 256 eden
	private final int BUFFER_SIZE = 256; // 1024*1;//1024*2都会down//1024*4 500M
											// 1024*8 700M -- 128 256M

	private ExecutorService executor;
	private RingBuffer<EventPackage> ringBuffer;
	private DisruptorHandler[] handlers;
	private WorkerPool<EventPackage> workerPool;
	private final boolean isError;
	private final BatchEventProcessor<EventPackage> sendProcessor;

	public DisruptorProducer(boolean isError) {
		this.isError = isError;
		Thread.currentThread().setName("duckula-producer");
		int threadnum = Main.context.getTask().getThreadNum();
		threadnum = threadnum == 0 ? 1 : threadnum;
		this.executor = Executors.newFixedThreadPool(threadnum + 1, DaemonThreadFactory.INSTANCE);
		ringBuffer = RingBuffer.createSingleProducer(EVENT_FACTORY, BUFFER_SIZE, new BlockingWaitStrategy());

		handlers = new DisruptorHandler[threadnum];
		for (int i = 0; i < threadnum; i++) {
			handlers[i] = new DisruptorHandler();
		}

		workerPool = new WorkerPool<EventPackage>(ringBuffer, ringBuffer.newBarrier(), new FatalExceptionHandler(),
				handlers);

		workerPool.start(executor);

		final SequenceBarrier sendBarrier = ringBuffer.newBarrier(workerPool.getWorkerSequences());
		JSONObject sendParams = new JSONObject();
		sendParams.put(ReceiveAbs.colParam, Main.context.getTask().getParams());
		sendParams.put(ReceiveAbs.colTaskId, Main.context.getTask().getId());
		sendParams.put(ReceiveAbs.colMiddlewareType, Main.context.getTask().getMiddlewareType().name());
		sendParams.put(ReceiveAbs.colMiddlewareInst, Main.context.getTask().getMiddlewareInst());
		final DisruptorSendHandler sendHandler = new DisruptorSendHandler(sendParams);
		sendProcessor = new BatchEventProcessor<EventPackage>(ringBuffer, sendBarrier, sendHandler);
		executor.submit(sendProcessor);

		ringBuffer.addGatingSequences(sendProcessor.getSequence());
	}

	/***
	 * 停止收场
	 */
	public void stop() {
		workerPool.drainAndHalt();
		sendProcessor.halt();
		executor.shutdown();
	}

	private final EventFactory<EventPackage> EVENT_FACTORY = new EventFactory<EventPackage>() {
		public EventPackage newInstance() {
			return new EventPackage();
		}
	};

	private long sequence;

	public EventPackage getNextBuild() {
		sequence = ringBuffer.next();
		EventPackage retobj = ringBuffer.get(sequence);
		retobj.setError(isError);
		return retobj;
	}

	public void sendMsg(EventPackage sendBean) {
		ringBuffer.publish(sequence);
	}

}
