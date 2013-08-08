package com.eqt.needle.topics.control;

import java.util.List;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import com.eqt.needle.notification.Control;
import com.eqt.needle.notification.Message;
import com.eqt.needle.notification.TopicConsumer;

/**
 * Tasks use this for listening
 */
public class TaskControlTopic extends TopicConsumer {

	public static final String TASK_CONTROL_TO_AM = "task_control_to_am";
	public static final String TASK_CONTROL_TO_TASK = "task_control_to_task";
	
	/**
	 * initializes a comm talking to the AM
	 * @param seedBrokers
	 */
	public TaskControlTopic(String seedBrokers) {
		super(TASK_CONTROL_TO_TASK, seedBrokers);
	}
		
	public void sendControl(Producer<String, String> prod, Control control) {
		prod.send(new KeyedMessage<String, String>(TASK_CONTROL_TO_AM,control.toString() ,""));
	}
	
	public Control getNextControl() {
		Message<String,String> message = getNextStringMessage();
		if(message != null) {
			return Control.valueOf(message.key);
		}
		return null;
	}
}
