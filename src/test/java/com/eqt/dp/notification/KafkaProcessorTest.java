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
	
	@Before
	public void setUp() throws Exception {
		Thread tz = new Thread(zk);
		tz.start();
		broker = new StandaloneBroker(zk.getURI());
		Thread tb = new Thread(broker);
		tb.start();
		
		server = new KafkaProcessor(broker.getURI(), broker.getZKURI(), false);
		client = new KafkaProcessor(broker.getURI(), zk.getURI());
		
	}

	@After
	public void tearDown() throws Exception {
		zk.shutdown();
		if(broker != null)
			broker.shutDown();
	}

	@Test
	public void test() {
		WorkUnit u = new WorkUnit();
		u.pathIn = "/pathIn";
		u.pathOut = "/pathOut";
		Control c = Control.START;
		server.sendControlEvent(u, c);
		Control controlEvent = client.getControlEvent();
		assertEquals(c, controlEvent);
	}

}
