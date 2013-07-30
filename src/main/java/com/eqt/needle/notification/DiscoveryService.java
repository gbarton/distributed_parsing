package com.eqt.needle.notification;

import java.util.HashMap;
import java.util.Map;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * This class allows the application to discover all the other
 * tasks running in this yarn app.
 */
public class DiscoveryService extends TopicConsumer {
	
	private static final String TOPIC = "discovery_topic";
	
	Map<String, Map<String,String>> services = new HashMap<String, Map<String,String>>();
	
	/**
	 * @param prod producer to announce presense too
	 * @param serviceId unique name of service.
	 */
	public DiscoveryService(Producer<String,String> prod, String serviceId, String brokerUri) {
		super(TOPIC,prod);
		String keyValue = "created:" + System.currentTimeMillis();
		prod.send(new KeyedMessage<String, String>(TOPIC, serviceId, keyValue));
		init(brokerUri);
	}
	
	
}
