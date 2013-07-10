package com.gman.notification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaProcessor extends BaseProcessor {
	private static final Log LOG = LogFactory.getLog(KafkaProcessor.class);
	//I still dont know why consumer wants zk and producer wants brokers
	private String brokerURI;
	private String BrokerZKURI;
	private final ConsumerConnector consumer;
	private final Producer<String, String> producer;
	private ArrayBlockingQueue<WorkUnit> workUnits = new ArrayBlockingQueue<EventProcessor.WorkUnit>(10);
	private ArrayBlockingQueue<Control> controlCommands = new ArrayBlockingQueue<EventProcessor.Control>(10);

	public KafkaProcessor(String broker, String zkuri) {
		this.brokerURI = broker;
		this.BrokerZKURI = zkuri;
		
		//consumer setup
		Properties cprops = new Properties();
		cprops.put("zookeeper.connect", BrokerZKURI);
        //TODO: do i need different group names? probably need to be unique
		cprops.put("group.id", "group1");
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
	}
	
	@Override
	public void sendControlEvent(WorkUnit work, Control c) {
		System.out.println("controlEvent: " + c + " unit: " + work);
	}
	
	@Override
	public Control getControlEvent() {
		return null;
	}
	
	@Override
	public void createdNewWork(WorkUnit from, String path) {
		System.out.println(from + " generated child: " + path);
	}
	
	@Override
	public WorkUnit getNextWorkUnit() {
		return null;
	}

	@Override
	public void run() {
//		ExecutorService executor;
//	    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//	    topicCountMap.put(topic, new Integer(a_numThreads));
//	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
//	    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
//	 
//	 
//	    // now launch all the threads
//	    executor = Executors.newFixedThreadPool(a_numThreads);
//	 
//	    // now create an object to consume the messages
//	    int threadNumber = 0;
//	    for (final KafkaStream stream : streams) {
//	        executor.submit(new ConsumerThread(stream, threadNumber));
//	        threadNumber++;
//	    }
	}
	
	/**
	 * Listens for the given object that is typed into
	 * 
	 * @param <T>
	 */
	public class ConsumerThread<T extends KMesg<T>> implements Runnable {
	    private KafkaStream<String, String> stream;
	    private int threadNum;
	    private T kMesg;
	    private ArrayBlockingQueue<T> queue;
	 
	    public ConsumerThread(KafkaStream stream, int threadNum, T obj,
	    		ArrayBlockingQueue<T> queue) {
	    	this.threadNum = threadNum;
	    	this.stream = stream;
	    	this.kMesg = obj;
	    	this.queue = queue;
	    }
	 
	    public void run() {
	        ConsumerIterator<String, String> it = stream.iterator();
	        while (it.hasNext()) {
	        	String message = it.next().message();
	            LOG.info("Thread " + threadNum + ": " + message);
	            try {
					T obj = (T) kMesg.getClass().newInstance();
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
