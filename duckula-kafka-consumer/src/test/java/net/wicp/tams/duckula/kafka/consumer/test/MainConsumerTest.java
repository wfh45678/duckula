package net.wicp.tams.duckula.kafka.consumer.test;

import static com.lmax.disruptor.RingBuffer.createMultiProducer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkProcessor;
import com.lmax.disruptor.util.DaemonThreadFactory;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.duckula.kafka.consumer.test.bean.EventConsumer;
import net.wicp.tams.duckula.kafka.consumer.test.handlerConsumer.BusiHander;
import net.wicp.tams.duckula.kafka.consumer.test.handlerConsumer.SendHander;

@Slf4j
public class MainConsumerTest {
	private static final int BUFFER_SIZE = 1024 * 8;

	// private final ExecutorService executor;// = Executors.newFixedThreadPool(8,
	// DaemonThreadFactory.INSTANCE);

	private final EventFactory<EventConsumer> EVENT_FACTORY = new EventFactory<EventConsumer>() {
		public EventConsumer newInstance() {
			return EventConsumer.builder().build();
		}
	};

	private final RingBuffer<EventConsumer> ringBuffer = createMultiProducer(EVENT_FACTORY, BUFFER_SIZE,
			new BusySpinWaitStrategy());

	public void init(String[] args) throws SQLException {
		Thread.currentThread().setName("Consumer-main");
		log.info("----------------------启动Disruptor-------------------------------------");
		disruptorRun();
		addShutdownHook();
		addTimer();
	}

	SequenceBarrier baseBarrier;
	SequenceBarrier busiBarrier;

	private void disruptorRun() {

		for (long i = 0; i < 10; i++) {
			long next = ringBuffer.next();
			ringBuffer.get(next).setData(i);
			ringBuffer.publish(next);
		}

		///////////////////////////////////////// 业务处理///////////////////////////////////////////////////////////////////////////////
		baseBarrier = ringBuffer.newBarrier();
		Sequence busiSequence = new Sequence(-1);
		int busiNum = 2;
		BusiHander[] busiHanders = new BusiHander[busiNum];
		for (int i = 0; i < busiHanders.length; i++) {
			busiHanders[i] = new BusiHander();
		}
		@SuppressWarnings("unchecked")
		WorkProcessor<EventConsumer>[] busiProcessors = new WorkProcessor[busiNum];
		for (int i = 0; i < busiProcessors.length; i++) {
			busiProcessors[i] = new WorkProcessor<EventConsumer>(ringBuffer, baseBarrier, busiHanders[i],
					new IgnoreExceptionHandler(), busiSequence);
		}
		///////////////////////////////////////// 发送处理///////////////////////////////////////////////////////////////////////////////
		busiBarrier = ringBuffer.newBarrier(getSeqAry(busiProcessors));
		Sequence sendSequence = new Sequence(-1);
		int sendNum = 2;
		SendHander[] sendHanders = new SendHander[sendNum];
		for (int i = 0; i < sendHanders.length; i++) {
			sendHanders[i] = new SendHander();
		}
		@SuppressWarnings("unchecked")
		WorkProcessor<EventConsumer>[] sendProcessors = new WorkProcessor[sendNum];
		for (int i = 0; i < sendProcessors.length; i++) {
			sendProcessors[i] = new WorkProcessor<EventConsumer>(ringBuffer, busiBarrier, sendHanders[i],
					new IgnoreExceptionHandler(), sendSequence);
		}
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////
		ringBuffer.addGatingSequences(getSeqAry(sendProcessors));
		
		
		
		ExecutorService executor = Executors.newFixedThreadPool(busiNum + sendNum, DaemonThreadFactory.INSTANCE);
		for (WorkProcessor<EventConsumer> busiProcessor : busiProcessors) {
			executor.submit(busiProcessor);
		}
		for (WorkProcessor<EventConsumer> sendProcessor : sendProcessors) {
			executor.submit(sendProcessor);
		}
	}

	private void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.info("----------------------执行关闭进程 钩子开始-------------------------------------");
				// DisruptorManager.getInst().stop(); // 为什么hold住？
				updateLastId();
				log.info("----------------------执行关闭进程 钩子完成-------------------------------------");
			}
		});
	}

	private void addTimer() {
		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		// 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
		service.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				updateLastId();
			}
		}, 10, 3, TimeUnit.SECONDS);
	}

	private void updateLastId() {
		System.out.println("aaaa");
	}

	public static void main(String[] args) throws IOException, SQLException {
		MainConsumerTest main = new MainConsumerTest();
		main.init(args);
		System.in.read();
	}

	private Sequence[] getSeqAry(WorkProcessor<EventConsumer>[] baseProcessors) {
		Sequence[] seqAry = new Sequence[baseProcessors.length];
		for (int i = 0; i < seqAry.length; i++) {
			seqAry[i] = baseProcessors[i].getSequence();
		}
		return seqAry;
	}
}
