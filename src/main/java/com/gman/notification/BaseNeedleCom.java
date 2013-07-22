package com.gman.notification;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;


/**
 * Provides basic initialization of a kafka producer.
 * TODO: get the consumer stuff moved into here as well.
 * TODO: cleanup c variable. 
 */

public abstract class BaseNeedleCom<K,V> {
	private static final Log LOG = LogFactory.getLog(BaseNeedleCom.class);

	// setting up consumers
	protected ExecutorService executor = null;
	protected ConsumerThread<K, V> c = null;
	// TODO: property drive
	protected int maxThreads = 1;
	protected String brokerURI;
	protected String brokerZKURI;
	protected Producer<String, String> producer;
	//topics to talk to and read from
	protected String topicTo = null;
	protected String topicFrom = null;
	
	public BaseNeedleCom(String broker, String zkuri, String topicTo, final String topicFrom) {
		this.brokerURI = broker;
		this.brokerZKURI = zkuri;
		this.topicTo = topicTo;
		this.topicFrom = topicFrom;
		setupProducer();

		//really here just to give me a nice name for the thread.
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
		LOG.info("BaseNeedleCommmunication online");
	}
	
	private void setupProducer() {
		//producer setup
		Properties pprops = new Properties();
		pprops.put("metadata.broker.list", brokerURI);
		pprops.put("serializer.class", "kafka.serializer.StringEncoder");
		pprops.put("partitioner.class", "com.gman.broker.SimplePartitioner");
		pprops.put("request.required.acks", "1");
		pprops.put("producer.type", "sync");
		ProducerConfig config = new ProducerConfig(pprops);
		producer = new Producer<String, String>(config);
		LOG.info("producer connector created");
	}
	
	public Message<K,V> getMessage() {
		return c.getNextMessage();
	}

	public abstract void sendMessage(Message<K,V> message);

	
}
