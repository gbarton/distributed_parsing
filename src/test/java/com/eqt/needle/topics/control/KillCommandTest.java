package com.eqt.needle.topics.control;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.eqt.needle.BasicYarnService;
import com.eqt.needle.broker.EmbeddedZK;
import com.eqt.needle.broker.StandaloneBroker;
import com.eqt.needle.notification.DiscoveryService;
import com.eqt.needle.notification.KafkaUtils;
import com.eqt.needle.notification.StatusReporter;
import com.eqt.needle.topics.control.KillCommand.WHOAMI;

import static org.junit.Assert.*;

public class KillCommandTest {

	EmbeddedZK zk = null;
	StandaloneBroker broker = null;

	@Before
	public void setUp() throws Exception {
		zk = new EmbeddedZK();
		Thread tz = new Thread(zk, "zk");
		tz.start();
		broker = new StandaloneBroker(zk.getURI());
		Thread tb = new Thread(broker, "broker");
		tb.start();
	}

	@After
	public void tearDown() throws Exception {
		if (broker != null)
			broker.shutDown();
		if (zk != null)
			zk.shutdown();
	}

	@Test
	/**
	 * Spawns a bunch of threads, then fires a kill command.
	 */
	public void testKillThreads() throws UnknownHostException, IOException, InterruptedException, ExecutionException {
		List<Future<Integer>> calls = new ArrayList<Future<Integer>>();
		ExecutorService service = Executors.newFixedThreadPool(5);

		for (int i = 0; i < 5; i++) {
			Callable<Integer> c = new TestA(broker.getURI(), i);
			Future<Integer> f = service.submit(c);
			calls.add(f);
		}

		// 1 2 skip a few
		Thread.sleep(1000);
		TestB b = new TestB(broker.getURI()); 
		System.out.println("killin begins!");
		b.kill();

		int down = 0;
		for (Future<Integer> f : calls) {
			down += f.get();
		}

		service.shutdown();
		b.close();
		assertEquals(5, down);
	}

	/**
	 * dumb class that spins until the close is called.
	 */
	public class TestA extends BasicYarnService implements Callable<Integer> {

		private boolean shutdown = false;

		public TestA(String broker, int num) throws IOException {
			super(num + "");
			this.brokerUri = broker;
			prod = KafkaUtils.getProducer(this.brokerUri);
			statusReporter = new StatusReporter(prod, this.brokerUri, status, false);
			discoveryService = new DiscoveryService(prod, "TEST", this.brokerUri);
			killThread = new KillCommand(WHOAMI.TASK, this.brokerUri, prod, this);
		}

		@Override
		public void close() {
			this.shutdown = true;
		}

		@Override
		public Integer call() throws Exception {
			System.out.println("running " + this.serviceName);
			while (!shutdown) {
				Thread.sleep(10);
			}
			return 1;
		}

	}
	
	/**
	 * smallest Master ever!
	 */
	public class TestB extends BasicYarnService {
		
		public TestB(String broker) throws IOException {
			super("MASTER_BLASTER");
			this.brokerUri = broker;
			prod = KafkaUtils.getProducer(this.brokerUri);
			killThread = new KillCommand(WHOAMI.AM, this.brokerUri, prod, this);
		}
		
		public void kill() {
			killThread.kill();
		}
	}
}
