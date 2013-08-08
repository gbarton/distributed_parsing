package com.eqt.needle.topics.control;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import com.eqt.needle.notification.Control;
import com.eqt.needle.notification.Message;
import com.eqt.needle.notification.TopicConsumer;

/**
 * Use this class to send Control messages to the AM (really mostly good for stopping, though
 * one could totally make the AM wait for a go signal)
 * @author gman
 */
public class ClientControlTopic extends TopicConsumer {
	
	public static final String CLIENT_CONTROL_TOPIC_TO_AM = "client_control_to_am";
	public static final String CLIENT_CONTROL_TOPIC_TO_CLIENT = "client_control_to_client";
	
	public ClientControlTopic(String brokerUri) {
		super(CLIENT_CONTROL_TOPIC_TO_CLIENT,brokerUri);
	}

	public void sendControl(Producer<String, String> prod, Control control) {
		prod.send(new KeyedMessage<String, String>(CLIENT_CONTROL_TOPIC_TO_AM,control.toString() ,""));
	}
	
	public Control getNextControl() {
		Message<String,String> message = getNextStringMessage();
		if(message != null) {
			return Control.valueOf(message.key);
		}
		return null;
	}
	
}
