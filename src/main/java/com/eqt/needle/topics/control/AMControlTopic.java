package com.eqt.needle.topics.control;

import com.eqt.needle.notification.Control;
import com.eqt.needle.notification.Message;
import com.eqt.needle.notification.TopicConsumer;

/**
 * Reads input from the client. 
 * TODO: this says AM at the moment, but all tasks and the AM 
 * are using this.. should I gracefully shutdown when client calls
 * stop??
 * @author gman
 *
 */
public class AMControlTopic extends TopicConsumer {

	public AMControlTopic(String brokerUri) {
		super(ClientControlTopic.CLIENT_CONTROL_TOPIC_TO_CLIENT,brokerUri);
	}
	
	public Control getNextControl() {
		Message<String,String> message = getNextStringMessage();
		if(message != null) {
			return Control.valueOf(message.key);
		}
		return null;
	}
	
}
