package com.eqt.dp.notification;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.gman.broker.EmbeddedZK;

public class KafkaProcessorTest {

	private final EmbeddedZK zk = new EmbeddedZK();
	
	@Before
	public void setUp() throws Exception {
		Thread t = new Thread(zk);
		t.start();

		
	}

	@After
	public void tearDown() throws Exception {
		zk.shutdown();
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}

}
