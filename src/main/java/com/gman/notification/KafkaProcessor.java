package com.gman.notification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.gman.notification.EventProcessor.WorkUnit;
import com.gman.util.Constants;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * This connects to the kafka brokers with producers and consumers.
 * TODO: do i want hardcoded streams or pass those in??
 * This class has 2 modes, the client side which is considered the DOWN side
 * of topics, and the server side which is the UP side.
 */
public class KafkaProcessor extends BaseProcessor {
	private static final Log LOG = LogFactory.getLog(KafkaProcessor.class);
	//I still dont know why consumer wants zk and producer wants brokers
	private String brokerURI;
	private String BrokerZKURI;
	//TODO: how long to chill when waiting for a message, probably should be configurable
	private int waitInSeconds = 5;
	private int numTopics = 1;
	private boolean isClient = true;
	private final ConsumerConnector consumer;
	private final Producer<String, String> producer;
	private ArrayBlockingQueue<WorkUnit> workUnits = new ArrayBlockingQueue<EventProcessor.WorkUnit>(10);
	private ArrayBlockingQueue<Control> controlCommands = new ArrayBlockingQueue<EventProcessor.Control>(10);

	public KafkaProcessor(String broker, String zkuri) {
		this(broker, zkuri,true);
	}
	
	/**
	 * @param broker
	 * @param zkuri
	 * @param isClient -override for master/server mode
	 */
	public KafkaProcessor(String broker, String zkuri, boolean isClient) {
		this.brokerURI = broker;
		this.BrokerZKURI = zkuri;
		this.isClient = isClient;
		LOG.info("Starting (" + (isClient?"client":"server") + ") init with broker: " + broker + " and zk: " + zkuri);
		
		//consumer setup
		Properties cprops = new Properties();
		cprops.put("zookeeper.connect", BrokerZKURI);
        //TODO: do i need different group names? probably need to be unique
		cprops.put("group.id", "group-" + System.currentTimeMillis());
		cprops.put("zookeeper.session.timeout.ms", "4000");
		cprops.put("zookeeper.sync.time.ms", "200");
		cprops.put("auto.commit.interval.ms", "1000");
        ConsumerConfig kconf = new ConsumerConfig(cprops);
		consumer = Consumer.createJavaConsumerConnector(kconf);
		LOG.info("consumer connector created");
		
		//producer setup
		Properties pprops = new Properties();
		pprops.put("metadata.broker.list", brokerURI);
		pprops.put("serializer.class", "kafka.serializer.StringEncoder");
		pprops.put("partitioner.class", "com.gman.broker.SimplePartitioner");
		pprops.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(pprops);
		producer = new Producer<String, String>(config);
		LOG.info("producer created");
		
		run();
//		Thread t = new Thread(this);
//		t.start();
	}
	
	
//	/**
//	 * This class has 2 modes, the client side which is considered the DOWN side
//	 * of topics, and the server side which is the UP side. This will startup
//	 * the correct streams for listening and writing to.
//	 * @param client
//	 */
//	public void setClient(boolean isClient) {
//		this.isClient = isClient;
//	}
	
	@Override
	public void sendControlEvent(WorkUnit work, Control c) {
		LOG.info("@@@@@@@@@@@@");
		LOG.info("topic: " + (isClient?Constants.TOPIC_CONTROL_UP:Constants.TOPIC_CONTROL_DOWN) + " controlEvent: " + c + " unit: " + work);
		LOG.info("@@@@@@@@@@@@");
		KeyedMessage<String, String> mess = new KeyedMessage<String, String>(
				isClient?Constants.TOPIC_CONTROL_UP:Constants.TOPIC_CONTROL_DOWN,
				c.pack(),work.pack());
		LOG.info("@@@@@@@@@@@@");
		producer.send(mess);
		LOG.info("@@@@@@@@@@@@");
		
	}
	
	@Override
	public Control getControlEvent() {
		try {
			LOG.info("getControlEvent!!");
			return controlCommands.poll(waitInSeconds, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOG.error("I cant read the streams!",e);
			return null;
		}
	}
	
	@Override
	public void createdNewWork(WorkUnit from, String path) {
		System.out.println(from + " generated child: " + path);
	}
	
	@Override
	public WorkUnit getNextWorkUnit() {
		return null;
	}

	private List<KafkaStream<byte[], byte[]>> getStreams(String topic, int threads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threads);
		return consumer.createMessageStreams(topicCountMap).get(topic);
	}
	
	/**
	 * Helper to make the dumb map for asking for topic streams
	 * defaults to a single thread per stream
	 * @param topics
	 * @return
	 */
	private Map<String, Integer> topicMap(String... topics) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		Integer i = new Integer(1);
		for(String t : topics)
			topicCountMap.put(t, i);
		return topicCountMap;
	}
	
	
	
	@Override
	public void run() {
		
		//setting up consumers
		ExecutorService executor = null;
		
		//2 topics to LISTEN to
		if(isClient) {
			numTopics = 2;
			executor = Executors.newFixedThreadPool(numTopics);

		    int threadNum = 0;
	    	//for each stream in topic
	    	for(final KafkaStream<byte[],byte[]> stream : getStreams(Constants.TOPIC_WORKUNIT_DOWN,1)) {
	    		executor.submit(new ConsumerThread<WorkUnit>(stream, threadNum, WorkUnit.class, workUnits,Constants.TOPIC_WORKUNIT_DOWN));
	    		threadNum++;
	    	}
	    	//for each stream in topic
	    	for(final KafkaStream<byte[],byte[]> stream : getStreams(Constants.TOPIC_CONTROL_DOWN,1)) {
	    		executor.submit(new ConsumerThread<Control>(stream, threadNum, Control.class, controlCommands,Constants.TOPIC_CONTROL_DOWN));
	    		threadNum++;
	    	}

		
		} else {
			numTopics = 2;
			executor = Executors.newFixedThreadPool(numTopics);
			int threadNum = 0;
	    	//for each stream in topic
	    	for(final KafkaStream<byte[],byte[]> stream : getStreams(Constants.TOPIC_CONTROL_UP,1)) {
	    		executor.submit(new ConsumerThread<Control>(stream, threadNum, Control.class, controlCommands,Constants.TOPIC_CONTROL_UP));
	    		threadNum++;
	    	}
		}
	}
	
	/**
	 * Listens for the given object that is typed into
	 * 
	 * @param <T>
	 */
	public class ConsumerThread<T extends KMesg<T>> implements Runnable {
	    private KafkaStream<byte[], byte[]> stream;
	    private int threadNum;
	    private Class<T> kMesg;
	    private ArrayBlockingQueue<T> queue;
	    private String topic;
	 
	    public ConsumerThread(KafkaStream<byte[], byte[]> stream, int threadNum, Class<T> obj,
	    		ArrayBlockingQueue<T> queue, String topic) {
	    	this.threadNum = threadNum;
	    	this.stream = stream;
	    	this.kMesg = obj;
	    	this.queue = queue;
	    	this.topic = topic;
	    }
	 
	    public void run() {
	        ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        LOG.info("*************");
	        LOG.info(stream.clientId() + " Consumer started with Class Type: " + kMesg.getName() + " on topic: " + topic);
	        LOG.info("*************");
	        while (it.hasNext()) {
	        	String message = new String(it.next().message());
		        LOG.info("*************");
	            LOG.info("Thread " + threadNum + ": " + message);
		        LOG.info("*************");
	            try {
					T obj = (T) kMesg.newInstance();
					obj.unpack(message);
					queue.add(obj);
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            
	        }
	        LOG.info("Shutting down Thread: " + threadNum);
	    }
	}
	
}
