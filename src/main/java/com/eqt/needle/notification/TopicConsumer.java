package com.eqt.needle.notification;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * Wrapper class for listening to all partitions of a topic.
 * NOTE: topic must exist for this consumer to work. Tries for a little while to find one
 * then blows up.
 * TODO: consider lazy loading the consumers on first request?? dunno.
 *
 */
public class TopicConsumer {
	private static final Log LOG = LogFactory.getLog(TopicConsumer.class);
	private String topic;
	private List<String> seedBrokers;
	private Set<HostPort> brokers;
	private int partitions = 0;
	private List<NeedleConsumer> consumers = new ArrayList<NeedleConsumer>();
	private int currCon = 0;
	private ExecutorService executor;
	
	private static final String initContents = "INITIAL_MESSAGE_HOPE_NO_ONE_MAKES_A_MESSAGE_LIKE_THIS";
	
	/**
	 * Will initialize the topic by sending an init message accross it.
	 * NOTE: you still must call one of the init() methods before using the consumers.
	 */
	public TopicConsumer(String topic, Producer<String,String> prod) {
		this.topic = topic;
		prod.send(new KeyedMessage<String, String>(topic, initContents,initContents));
	}
	
	/***
	 * @param topic
	 * @param brokers can be a , separated host:port list of brokers
	 */
	protected void init(String brokers) {
		String[] b = brokers.split(",");
		List<String> l = new ArrayList<String>();
		for(String s : b)
			l.add(s);
		init(l);
	}
	
	protected void init(List<String> seedBrokers) {
		this.seedBrokers = seedBrokers;
		this.brokers = HostPort.build(seedBrokers);
		
		this.partitions = findPartitionCount();
		
		int tries = 10;
		
		//start the loop
		while(this.partitions == 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.warn("cant sleep well");
			}
			tries++;
			if(tries == 10)
				throw new IllegalStateException("just cant find any partitions for topic: " + topic);
			LOG.info("looking for partitons");
			this.partitions = findPartitionCount();
		}
		LOG.info("topicConsumer found partitions for topic: " + topic + " with " + this.partitions + " partitions");
		
		//really here just to give me a nice name for the thread.
		executor = new ThreadPoolExecutor(partitions, // core thread pool size
				partitions, // maximum thread pool size
							// this
				1, // time to wait before resizing pool
				TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(partitions, true), new ThreadFactory() {
					int num = 0;

					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "consumer-" + topic + "-" + num++);
					}
				});
		
		for(int i=0; i < partitions;i++) {
			NeedleConsumer consumer = new NeedleConsumer(topic, i, this.seedBrokers);
			consumers.add(consumer);
			executor.submit(consumer);
		}
		LOG.info("all consumers started for topic " + topic);
	}
	
	public void close() {
		for(NeedleConsumer c : consumers)
			c.close();
		executor.shutdown();
	}
	
	/**
	 * will round robin through all the partitions seeing if it can find a 
	 * message to return, if it gets through all of them without finding one it returns
	 * null.
	 * Returns the raw String version of the message sent, typically other
	 * classes will extends and provide their own getNextMessage() with their
	 * desired return types.
	 * @return
	 */
	public Message<String,String> getNextStringMessage() {
		//start at last consumer pulled from
		int tries = 0;
		Message<String,String> message = null;
		while(tries < partitions) {
			message = check(consumers.get(currCon).getNextMessage());
			if(currCon == partitions -1)
				currCon = 0;
			else
				currCon++;
			if(message != null)
				break;
			tries++;
		}
		
		return message;
	}
	
	//Checks to see if this was the init message we saw, if it was, eat it.
	private Message<String,String> check(Message<String,String> m) {
		if(m == null)
			return m;
		if(initContents.equalsIgnoreCase(m.key) && initContents.equalsIgnoreCase(m.value))
			return null;
		return m;
	}
	
	
	/**
	 * goes and pulls the info for a given topic and gets the number of partitions associated with it.
	 * @param brokers
	 * @param topic
	 * @return
	 */
	private int findPartitionCount() {
		int numPartitions = 0;

		for (HostPort hp : brokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(hp.host, hp.port, 100000, 64 * 1024, "leaderLookup");
				List<String> topics = new ArrayList<String>();
				topics.add(topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
				
				//really his will only have 1 entry since we just asked for a single topic
				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData)//always 1
					numPartitions += item.partitionsMetadata().size();
				
			} catch (Exception e) {
				LOG.error("Error communicating with Broker [" + hp + "] to find Leader for [" + topic + "] Reason: ", e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		return numPartitions;
	}

}
