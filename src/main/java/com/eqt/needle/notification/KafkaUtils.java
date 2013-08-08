package com.eqt.needle.notification;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaUtils {
	private static final Log LOG = LogFactory.getLog(KafkaUtils.class);

	public static Producer<String,String> getProducer(String brokerURI) {
		//producer setup
		LOG.info("producer connector begin init");
		Properties pprops = new Properties();
		pprops.put("metadata.broker.list", brokerURI);
		pprops.put("serializer.class", "kafka.serializer.StringEncoder");
		pprops.put("partitioner.class", "com.eqt.needle.broker.SimplePartitioner");
		pprops.put("request.required.acks", "1");
		pprops.put("producer.type", "sync");
		ProducerConfig config = new ProducerConfig(pprops);
		Producer<String, String> producer = new Producer<String, String>(config);
		LOG.info("producer connector created");
		return producer;
	}
	
}
