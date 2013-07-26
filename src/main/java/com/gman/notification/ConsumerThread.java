package com.gman.notification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.gman.util.Constants;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public abstract class ConsumerThread<K,V> implements Runnable {
	private static final Log LOG = LogFactory.getLog(ConsumerThread.class);
	ArrayBlockingQueue<Message<K,V>> queue = null;
	private String topic;
	private String brokerZKURI;
	private int timeOut = 5;
	private AtomicBoolean running = new AtomicBoolean(false);
	
	public ConsumerThread(String topic, String brokerZKURI) {
		this.topic = topic;
		this.brokerZKURI = brokerZKURI;
		queue = new ArrayBlockingQueue<Message<K,V>>(10);
	}

	public abstract K readKey(byte[] key);
	public abstract V readValue(byte[] value);
	
	public Message<K,V> getNextMessage(int waitForSeconds) {
		init();
		try {
			LOG.info("waiting for message");
			//TODO: should be property driven
			return queue.poll(timeOut, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			//TODO: does/should this junk the consumer??
			LOG.info("FYI I got interupted waiting to read a message");
		}
		return null;
	}
	
	//TODO: this is crappy having this exposed
	public void init() {
		while (!running.get()) {
			try {
				Thread.sleep(500);
				LOG.info("request desired while I'm not initialized yet.");
			} catch (InterruptedException e) {
			}
		}
	}
	
	public Message<K,V> getNextMessage() {
		init();
		return getNextMessage(timeOut);
	}
	
	private List<KafkaStream<byte[], byte[]>> getStreams(String topic, int threads) {
		//consumer setup
		Properties cprops = new Properties();
		cprops.put("zookeeper.connect", brokerZKURI);
//		cprops.put("metadata.broker.list", System.getenv(Constants.BROKER_URI));
        //TODO: do i need different group names? probably need to be unique
		cprops.put("group.id", "group-" + System.currentTimeMillis());
		cprops.put("zookeeper.session.timeout.ms", "4000");
		cprops.put("zookeeper.sync.time.ms", "200");
		cprops.put("auto.commit.interval.ms", "1000");
        ConsumerConfig kconf = new ConsumerConfig(cprops);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(kconf);
		LOG.info("consumer connector created for topic: " + topic);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threads);
		return consumer.createMessageStreams(topicCountMap).get(topic);
	}
	
	@Override
	public void run() {
		LOG.info("");
		LOG.info("");
		LOG.info("started Consumer thread for topic: " + topic);
		List<KafkaStream<byte[],byte[]>> streams = getStreams(topic,1);
		KafkaStream<byte[], byte[]> stream = streams.get(0);
		ConsumerIterator<byte[],byte[]> it = stream.iterator();
		running.set(true);
		LOG.info("initialization lock lifted for topic: " + topic);
		
        while (it.hasNext()) {
        	MessageAndMetadata<byte[],byte[]> next = it.next();
//        	String message = new String(it.next().message());
        	LOG.info("A");
        	LOG.info("A");
        	LOG.info("A");
        	Message<K,V> m = new Message<K, V>(readKey(next.key()), readValue(next.message()));
        	queue.add(m);
        }
	}
	
	public void shutdown() {
		//no op atm
	}
	
}