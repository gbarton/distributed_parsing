package com.eqt.needle.notification;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * This class allows the application to discover all the other
 * tasks running in this yarn app. Keeps a map of maps of properties per service.
 * NOTE: service best be unique!
 */
public class DiscoveryService extends TopicConsumer {
	
	private static final String TOPIC = "discovery_topic";
	
	Map<String, Map<String,String>> services = new HashMap<String, Map<String,String>>();
	
	/**
	 * sends a message out saying that the service was created.
	 * @param prod producer to announce presence too
	 * @param serviceId unique name of service.
	 */
	public DiscoveryService(Producer<String,String> prod, String serviceId, String brokerUri) {
		super(TOPIC,brokerUri);
		String keyValue = "created:" + System.currentTimeMillis();
		prod.send(new KeyedMessage<String, String>(TOPIC, serviceId, keyValue));
	}
	
	public void sendInfo(Producer<String,String> prod, String service, String key, String value) {
		String keyVal = key + ":" + value;
		prod.send(new KeyedMessage<String, String>(TOPIC, service, keyVal));
	}
	
	public Set<String> getServices() {
		return services.keySet();
	}
	
	/**
	 * Pulls the property asked for for the requested service
	 * @param service
	 * @param propName
	 * @return
	 */
	public String getProp(String service, String propName) {
		if(services.containsKey(service))
			return services.get(service).get(propName);
		return null;
	}
	
	
	/**
	 * few basic property values.
	 * @author gman
	 */
	public static enum SERVICE {
		HOST,	//what host the service is running on
		PORT,	//if theres a port associated with the service
		UNIQUE_TOPIC //if there is a topic that this service is listening on specifically.
		
	}
	
}
