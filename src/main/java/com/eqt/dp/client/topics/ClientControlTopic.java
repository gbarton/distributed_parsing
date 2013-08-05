package com.eqt.dp.client.topics;

import kafka.javaapi.producer.Producer;

import com.eqt.needle.notification.TopicConsumer;

public class ClientControlTopic extends TopicConsumer {
	
	public static final String CLIENT_CONTROL_TOPIC_TO_AM = "client_control_to_am";
	public static final String CLIENT_CONTROL_TOPIC_TO_CLIENT = "client_control_to_client";
	
	public ClientControlTopic(Producer<String,String> prod,String brokerUri) {
		super(CLIENT_CONTROL_TOPIC_TO_AM,brokerUri);
	}

	
	
}
