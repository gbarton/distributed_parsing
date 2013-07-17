package com.gman.notification;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class Consumer<K,V> {
	private static final Log LOG = LogFactory.getLog(Consumer.class);

	// setting up consumers
	private ExecutorService executor = null;
	ConsumerThread<K, V> c = null;
	// TODO: property drive
	private int maxThreads = 1;
	private String brokerURI;
	private String brokerZKURI;
	private final Producer<String, String> producer;
	// topics to talk to and read from
	private String topicTo = null;
	private String topicFrom = null;

	public Consumer(String broker, String zkuri, String topicTo, final String topicFrom,ConsumerThread<K, V> c) {
		this.brokerURI = broker;
		this.brokerZKURI = zkuri;
		this.topicTo = topicTo;
		this.topicFrom = topicFrom;
		this.c = c;

		// producer setup
		Properties pprops = new Properties();
		pprops.put("metadata.broker.list", brokerURI);
		pprops.put("serializer.class", "kafka.serializer.StringEncoder");
		pprops.put("partitioner.class", "com.gman.broker.SimplePartitioner");
		pprops.put("request.required.acks", "1");
		pprops.put("producer.type", "sync");
		ProducerConfig config = new ProducerConfig(pprops);
		producer = new Producer<String, String>(config);
		LOG.info("producer connector created");

		// really here just to give me a nice name for the thread.
		executor = new ThreadPoolExecutor(1, // core thread pool size
				maxThreads, // maximum thread pool size
							// this
				1, // time to wait before resizing pool
				TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(maxThreads, true), new ThreadFactory() {
					int num = 0;

					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "consumer-" + topicFrom + "-" + num++);
					}
				});



		executor.submit(c);
		c.init();
	}

	public Message<K, V> getMessage() {
		LOG.info("%%%%%%%%%%%%%% " + topicFrom);
		Message<K, V> m = c.getNextMessage();
		LOG.info("message pulled: " + m);
		LOG.info("%%%%%%%%%%%%%% " + topicFrom);
		return m;
	}

	public void sendMessage(Message<K, V> message) {
		sendMessage(message.key, message.value);
	}

	private void sendMessage(K k, V v) {
		ObjectMapper mapper = new ObjectMapper();
		LOG.info("@@@@@@@@@@@@");
		LOG.info("topic: " + topicTo + " controlEvent: " + k + " unit: " + v);
		LOG.info("@@@@@@@@@@@@");
		KeyedMessage<String, String> mess;
		try {
			mess = new KeyedMessage<String, String>(topicTo, mapper.writeValueAsString(k),
					mapper.writeValueAsString(v));
			LOG.info("@@@@@@@@@@@@");
			producer.send(mess);
			LOG.info("@@@@@@@@@@@@");
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}