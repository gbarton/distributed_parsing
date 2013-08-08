package com.eqt.needle.topics.control;

import java.util.List;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import com.eqt.needle.notification.Control;
import com.eqt.needle.notification.Message;
import com.eqt.needle.notification.TopicConsumer;

/**
 * Am getting pretty sick of this pattern, perhaps StatusReporter is a better
 * way after all. This is for the AM to control all tasks.
 */
public class AMTaskControlTopic extends TopicConsumer {

	public AMTaskControlTopic(String seedBrokers) {
		super(TaskControlTopic.TASK_CONTROL_TO_AM, seedBrokers);
	}

	public void sendControl(Producer<String, String> prod, Control control, String message) {
		prod.send(new KeyedMessage<String, String>(TaskControlTopic.TASK_CONTROL_TO_TASK, control.toString(), message));
	}

	public Control getNextControl() {
		Message<String, String> message = getNextStringMessage();
		if (message != null) {
			return Control.valueOf(message.key);
		}
		return null;
	}

}
