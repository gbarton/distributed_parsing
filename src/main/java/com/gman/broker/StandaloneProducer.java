package com.gman.broker;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StandaloneProducer {
	private static final Log LOG = LogFactory.getLog(StandaloneProducer.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", "gman-minty:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.gman.broker.SimplePartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> producer = new Producer<String, String>(config);
		LOG.info("producer created");
		KeyedMessage<String, String> data = new KeyedMessage<String, String>("bla", "key1", "value1");

		try {
			producer.send(data);
		} catch(Exception e) {
			LOG.info("failing to send data, tryin again");
			producer.send(data);
		}
		LOG.info("wrote message: " + data);


	}

}
