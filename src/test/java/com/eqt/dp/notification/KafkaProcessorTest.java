package com.eqt.dp.notification;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.gman.broker.EmbeddedZK;
import com.gman.broker.StandaloneBroker;
import com.gman.notification.EventProcessor.Control;
import com.gman.notification.KafkaProcessor;
import com.gman.notification.EventProcessor.WorkUnit;

public class KafkaProcessorTest {

	private final EmbeddedZK zk = new EmbeddedZK();
	StandaloneBroker broker = null;

	KafkaProcessor server = null;
	KafkaProcessor client = null;
	WorkUnit u = new WorkUnit("/pathIn","/pathOut");
	Control c = Control.START;
	
	@Before
	public void setUp() throws Exception {
		Thread tz = new Thread(zk,"zk");
		tz.start();
		broker = new StandaloneBroker(zk.getURI());
		Thread tb = new Thread(broker,"broker");
		tb.start();
		
		
		server = new KafkaProcessor(broker.getURI(), broker.getZKURI(), false);
		client = new KafkaProcessor(broker.getURI(), broker.getZKURI());

		
//		server = new KafkaProcessor("localhost:9092", "localhost:2181/kafkatest", false);
//		client = new KafkaProcessor("localhost:9092", "localhost:2181/kafkatest");

	}

	@After
	public void tearDown() throws Exception {
		server.shutdown();
		client.shutdown();
		if(zk != null)
			zk.shutdown();
		if(broker != null)
			broker.shutDown();
	}

	@Test
	public void test() {

		server.sendControlEvent(u, c);
//		server.shutdown();
		Control controlEvent = client.getControlEvent();
//		assertEquals(c, controlEvent);
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
