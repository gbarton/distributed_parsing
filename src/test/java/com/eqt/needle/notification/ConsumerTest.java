package com.eqt.needle.notification;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.eqt.needle.broker.EmbeddedZK;
import com.eqt.needle.broker.StandaloneBroker;
import com.gman.notification.ClientControl;
import com.gman.notification.ServerControl;
import com.gman.notification.EventProcessor.WorkUnit;

public class ConsumerTest {
	EmbeddedZK zk = null;
	StandaloneBroker broker = null;
	WorkUnit u = new WorkUnit("/pathIn","/pathOut");
	Control c = Control.START;
	ServerControl ser = null;
	ClientControl con = null;

	@Before
	public void setUp() throws Exception {
		zk = new EmbeddedZK();
		Thread tz = new Thread(zk,"zk");
		tz.start();
		broker = new StandaloneBroker(zk.getURI());
		Thread tb = new Thread(broker,"broker");
		tb.start();
	}
	
	@After
	public void tearDown() throws Exception {
		if(broker != null)
			broker.shutDown();
		if(zk != null)
			zk.shutdown();
	}
	

	@Test
	public void testMultiMessageSendAnReceive() throws UnknownHostException, InterruptedException {
		String topic = "test";
		int total = 50;
		int sent = 0;
		
		Producer<String, String> prod = KafkaUtils.getProducer(broker.getURI());

		TopicConsumer tc = new TopicConsumer(topic,broker.getURI());
		int got = 0;
		int tries = total;
		while(got != total) {
			if(sent < total)
				prod.send(new KeyedMessage<String, String>(topic,"bla", "bla-"+sent++));

			Message<String,String> message = tc.getNextStringMessage();
			if(message != null)
				got++;
			else {
				Thread.sleep(100);
				tries--;
			}
			if(tries == 0)
				break;
		}
		tc.close();
		assertEquals(sent, got);
	}
	
	@Test
	public void testMultiMessage() throws UnknownHostException, InterruptedException {
		String topic = "test";
		int sent = 50;
		TopicConsumer tc = new TopicConsumer(topic,broker.getURI());
		Producer<String, String> prod = KafkaUtils.getProducer(broker.getURI());

		for(int i = 0; i <= sent;i++) {
			prod.send(new KeyedMessage<String, String>(topic,"bla", "bla-"+i));
		}
		
		int got = 0;
		int tries = sent;
		while(got != sent) {
			Message<String,String> message = tc.getNextStringMessage();
			if(message != null)
				got++;
			else {
				Thread.sleep(1000);
				tries--;
			}
			if(tries == 0)
				break;
		}
		assertEquals(sent, got);
	}
}
