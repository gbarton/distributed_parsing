package com.gman.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.gman.util.Constants;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class YarnParser implements Runnable {
	private static final Log LOG = LogFactory.getLog(YarnParser.class);
	private KafkaStream<byte[], byte[]> stream;
    private int threadNum = 1;
    private final ConsumerConnector consumer;
    private Map<String, String> envs;
    
    //TODO: hardcodes bad
    private String topic = "work";
    
	//TODO: generic consumer for getting work requests
	//TODO: generic producer for notifying of new work created.
	
	public YarnParser() throws Exception {
		LOG.info("Parser coming online");
		envs = System.getenv();
		
		String broker = envs.get(Constants.BROKER_ZK_URI);
		//lets you do this locally.
		if(broker == null)
			if(System.getProperty(Constants.BROKER_ZK_URI) == null)
				throw new Exception("well I dont know what to talk to anymore.");
			else
				broker = System.getProperty(Constants.BROKER_ZK_URI);
		
        Properties props = new Properties();
        props.put("zookeeper.connect", broker);
        //TODO: do i need different group names?
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig kconf = new ConsumerConfig(props);
		consumer = Consumer.createJavaConsumerConnector(kconf);
		LOG.info("Parser consumer created");
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		//single consumer assumption, will receive all messages.
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        stream = streams.get(0);
	}

	@Override
	public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
        	MessageAndMetadata<byte[],byte[]> m = it.next();
            LOG.info("Thread " + threadNum + ": " + new String(m.message()));
        }
        LOG.info("Shutting down Thread: " + threadNum);
		
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		
		System.setProperty(Constants.BROKER_ZK_URI, "localhost:2181/kafkatest");
		YarnParser parser = new YarnParser();
		parser.run();
		
	}


}
